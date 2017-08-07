package fr.jrds.opentsdbtore;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.Timer;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import jrds.Probe;
import jrds.PropertiesManager;
import jrds.Util;
import jrds.store.AbstractStoreFactory;

public class OpentsdbStoreFactory extends AbstractStoreFactory<OpentsdbStore> {

    private static final JsonFactory factory = new JsonFactory();
    private static final ThreadLocal<ObjectMapper> json = new ThreadLocal<ObjectMapper>() {
        @Override
        protected ObjectMapper initialValue() {
            return new ObjectMapper(factory)
                    .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
                    .configure(JsonGenerator.Feature.ESCAPE_NON_ASCII, true);
        }
    };

    private final Timer timer = new Timer("OpenTSDBTimer", true);
    private OpenTsdbConnexion cnx;

    @Override
    public OpentsdbStore create(Probe<?, ?> p) {
        return new OpentsdbStore(p, cnx);
    }

    @Override
    public void configureStore(PropertiesManager pm, Properties props) {
        try {
            super.configureStore(pm, props);

            String portStr = props.getProperty("port", "4242");
            int port = Util.parseStringNumber(portStr, new Integer(4242));

            String publishersStr = props.getProperty("publishers", "2");
            int publishers = Util.parseStringNumber(publishersStr, new Integer(2));

            String timeoutStr = props.getProperty("timeout", "10");
            int timeout = Util.parseStringNumber(timeoutStr, new Integer(10));

            String bufferSizeStr = props.getProperty("buffer", "20");
            int buffersize = Util.parseStringNumber(bufferSizeStr, new Integer(20));

            String serversStr = props.getProperty("servers","localhost");
            URI[] servers = buildUri(serversStr.split(";"), port);

            this.cnx = new OpenTsdbConnexion(json, servers, buffersize, publishers, timeout, timer);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private URI[] buildUri(String[] destinations, int port) throws URISyntaxException {
        URI[] servers = new URI[destinations.length];
        for(int i = 0; i < destinations.length ; i++) {
            String destination = destinations[i];
            if ( !destination.contains("//") ) {
                destination = "//" + destination;
            }
            URI newEndPoint = new URI(destination);
            int localport = port;
            if ("http".equals(newEndPoint.getScheme()) && newEndPoint.getPort() <= 0) {
                // if http was given, and not port specified, the user expected default port
                localport = port;
            }
            servers[i] = new URI(
                    (newEndPoint.getScheme() != null  ? newEndPoint.getScheme() : "http"),
                    null,
                    (newEndPoint.getHost() != null ? newEndPoint.getHost() : "localhost"),
                    (newEndPoint.getPort() > 0 ? newEndPoint.getPort() : localport),
                    (newEndPoint.getPath() != null ? newEndPoint.getPath() : ""),
                    null,
                    null
                    );
        }
        return servers;
    }

}
