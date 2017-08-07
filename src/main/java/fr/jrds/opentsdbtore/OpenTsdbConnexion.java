package fr.jrds.opentsdbtore;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.EntityBuilder;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.ConnectionPoolTimeoutException;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHttpEntityEnclosingRequest;
import org.apache.http.protocol.HttpContext;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import jrds.JrdsSample;
import jrds.Probe;
import jrds.Util;

public class OpenTsdbConnexion {

    private static final Logger logger = Logger.getLogger(OpenTsdbConnexion.class);

    private final ArrayBlockingQueue<JrdsSample> bulkqueue;
    private final int buffersize;
    private final CloseableHttpClient client;
    private final ThreadLocal<ObjectMapper> json;
    private final URI[] servers;
    private final Timer timer;

    public OpenTsdbConnexion(ThreadLocal<ObjectMapper> json, URI[] servers, int buffersize, int publishers, int timeout, Timer timer) {
        this.json = json;
        this.buffersize = buffersize;
        this.servers = servers;
        bulkqueue = new ArrayBlockingQueue<JrdsSample>((int) (buffersize * 1.5));
        client = httpConnexion(publishers, timeout);
        buildPublisher(publishers);
        TimerTask collector = new TimerTask () {
            public void run() {
                logger.debug(Util.delayedFormatString("Flushing %d samples", bulkqueue.size()));
                synchronized(bulkqueue) {
                    bulkqueue.notify();
                }
            }
        };
        timer.scheduleAtFixedRate(collector, 5000, 5000);
        this.timer = timer;
    }

    private CloseableHttpClient httpConnexion(int publishers, int timeout) {
        // The HTTP connection management
        HttpClientBuilder builder = HttpClientBuilder.create();

        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        cm.setDefaultMaxPerRoute(2);
        cm.setMaxTotal( 2 * publishers);
        cm.setValidateAfterInactivity(timeout * 1000);
        builder.setConnectionManager(cm);

        builder.setDefaultRequestConfig(RequestConfig.custom()
                .setConnectionRequestTimeout(timeout * 1000)
                .setConnectTimeout(timeout * 1000)
                .setSocketTimeout(timeout * 1000)
                .build());
        builder.setDefaultSocketConfig(SocketConfig.custom()
                .setTcpNoDelay(true)
                .setSoKeepAlive(true)
                .setSoTimeout(timeout * 1000)
                .build());
        builder.setDefaultConnectionConfig(ConnectionConfig.custom()
                .setCharset(Charset.forName("UTF-8"))
                .build());
        builder.setUserAgent("JRDS/OpenTsdbStoreClient 0.1");
        builder.setRetryHandler(new HttpRequestRetryHandler() {
            @Override
            public boolean retryRequest(IOException exception,
                    int executionCount, HttpContext context) {
                return false;
            }
        });
        return builder.build();
    }

    private void buildPublisher(int publishers) {
        for (int i = 1 ; i <= publishers ; i++) {
            Thread tp = new Thread(){
                @Override
                public void run() {
                    try {
                        while (!isInterrupted()) {
                            synchronized (bulkqueue) {
                                bulkqueue.wait();
                            }
                            OpenTsdbConnexion.this.publish();
                        }
                    } catch (InterruptedException e) {
                        TimerTask collector = new TimerTask () {
                            public void run() {
                                OpenTsdbConnexion.this.publish();
                            }
                        };
                        timer.schedule(collector, 0);
                        timer.cancel();
                        Thread.currentThread().interrupt();
                    }
                }
            };
            tp.setDaemon(true);
            tp.setName("OpenTSDBPublisher" + i);
            tp.start();
        }
    }

    public void newSample(JrdsSample sample) {
        while ( ! bulkqueue.offer(sample)) {
            // If queue full, launch a bulk publication
            synchronized (bulkqueue) {
                bulkqueue.notify();
                Thread.yield();
            }
        }
        // if queue reached publication size, publish
        if (bulkqueue.size() > buffersize) {
            synchronized (bulkqueue) {
                bulkqueue.notify();
            }
        }
    }

    public void flush() {
        publish();
    }

    private void publish() {
        List<JrdsSample> waiting = new ArrayList<>();
        JrdsSample o;
        int i = 0;
        while ((o = bulkqueue.poll()) != null && i < buffersize * 1.5) {
            waiting.add(o);
        }
        // It might received spurious notifications, so send only when needed
        if (waiting.size() > 0) {
            Object response = bulkstore(waiting);
            logger.debug(Util.delayedFormatString("response from OpenTSDB: {}", response));
        }
    }

    private Object bulkstore(List<JrdsSample> waiting) {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        List<Object> samples = new ArrayList<>();
        try {
            for(JrdsSample doc: waiting) {
                for(Entry<String, Number> e: doc.entrySet()) {
                    Map<String, Object> sample = new HashMap<>(3);
                    sample.put("metric", validate(doc.getProbe().getName()) + "." + validate(e.getKey()));
                    sample.put("timestamp", doc.getTime().getTime() / 1000);
                    sample.put("value", e.getValue());
                    Map<String, String> tags = new HashMap<>();
                    tags.put("host", validate(doc.getProbe().getHost().getName()));
                    tags.put("probe", validate(doc.getProbe().getName()));
                    String label = doc.getProbe().getLabel();
                    if (label != null) {
                        tags.put("label", validate(label));
                    }
                    sample.put("tags", tags);
                    doc.getProbe().getTags().forEach( t -> tags.put("probe.tags." + validate(t), "true"));
                    samples.add(sample);
                }
            }
            try {
                ObjectMapper jsonmapper = json.get();
                buffer.write(jsonmapper.writeValueAsBytes(samples));
            } catch (JsonProcessingException e) {
            }
            buffer.flush();
        } catch (IOException e1) {
            // Unreachable exception, no IO exception on ByteArrayOutputStream
        }
        HttpEntity content = EntityBuilder.create()
                .setBinary(buffer.toByteArray())
                .setContentType(ContentType.APPLICATION_JSON)
                .gzipCompress()
                .build();
        return doQuery(content, "POST", "/api/put");
    }

    private String validate(String in){
        return in.replace(' ', '_').replace(':', '.');
    }

    private <T> T doQuery(HttpEntity content, String verb, String path) {
        CloseableHttpResponse response = null;

        int tryExecute = 0;
        HttpHost host;
        do {
            URI tryURI = servers[ThreadLocalRandom.current().nextInt(servers.length)];
            BasicHttpEntityEnclosingRequest request = new BasicHttpEntityEnclosingRequest(
                    verb, tryURI.getPath() + path,
                    HttpVersion.HTTP_1_1);
            request.setEntity(content);
            host = new HttpHost(tryURI.getHost(), tryURI.getPort(), tryURI.getScheme());
            try {
                response = client.execute(host, request);
            } catch (ConnectionPoolTimeoutException e) {
                logger.error(Util.delayedFormatString("connection to %s timed out", host));
                tryExecute++;
            } catch (HttpHostConnectException e) {
                try {
                    throw e.getCause();
                } catch (ConnectException e1) {
                    logger.error(Util.delayedFormatString("connection to %s refused", host));
                } catch (SocketTimeoutException e1) {
                    logger.error(Util.delayedFormatString("slow response from %s", host));
                } catch (Throwable e1) {
                    logger.error(Util.delayedFormatString("connection to %s failed: %s", host, e1.getMessage()), e1);
                }
                tryExecute++;
            } catch (IOException e) {
                tryExecute++;
                logger.error(Util.delayedFormatString("Comunication with %s failed: %s", host, e.getMessage()));
            }
        } while (response == null && tryExecute < 5);
        if (response == null) {
            logger.error(Util.delayedFormatString("give up trying to connect to OpenTSDB"));
            return null;
        };
        StatusLine status = response.getStatusLine();
        int statusCode = status.getStatusCode();
        if (statusCode/10 != 20) {
            logger.error(Util.delayedFormatString("%s failed: %s %s", host, statusCode, status.getReasonPhrase()));
        }
        HttpEntity resultBody = response.getEntity();
        String contentType = null;
        String charset = Charset.defaultCharset().name();
        Header contentTypeHeader = null;
        if (resultBody != null) {
            contentTypeHeader = resultBody.getContentType();
            if(contentTypeHeader != null) {
                String value = contentTypeHeader.getValue();
                int pos = value.indexOf(';');
                if(pos > 0) {
                    contentType = value.substring(0, value.indexOf(';')).trim();
                    charset = value.substring(value.indexOf(';') + 1, value.length()).replace("charset=", "").trim();
                } else {
                    contentType = value;
                }
            }
        }
        if (statusCode == 204) {
            return null;
        } else if (contentType != null && ContentType.APPLICATION_JSON.getMimeType().equals(contentType)) {
            try (Reader contentReader = new InputStreamReader(resultBody.getContent(), charset)) {
                @SuppressWarnings("unchecked")
                T o = (T) json.get().readValue(contentReader, Object.class);
                try {
                    response.close();
                } catch (IOException e) {
                }
                return o;
            } catch (UnsupportedOperationException | IOException e) {
                logger.error(Util.delayedFormatString("error reading response content from %s: %s", host, e.getMessage()));
                return null;
            }
        } else {
            logger.error(Util.delayedFormatString("bad response content from %s: %s %s", host, status.getStatusCode(), status.getReasonPhrase()));
            try {
                response.close();
            } catch (IOException e) {
            }
            return null;
        }
    }

    public String getPrefix(Probe<?, ?> probe) {
        String pname = probe.getName();
        if(pname.contains(".")) {
            pname = pname.replace('.', '+');
        }
        if(pname.contains(" ")) {
            pname = pname.replace(' ', '_');
        }
        return pname;
    }

}
