package fr.jrds.opentsdbtore;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.hbase.async.HBaseException;

import com.stumbleupon.async.Deferred;

import jrds.ArchivesSet;
import jrds.JrdsSample;
import jrds.Probe;
import jrds.store.AbstractStore;
import jrds.store.EmptyExtractor;
import jrds.store.Extractor;
import net.opentsdb.core.TSDB;

public class OpentsdbStore extends AbstractStore<TSDB> {
    private final TSDB tsdbConnection;

    public OpentsdbStore(Probe<?, ?> p, TSDB tsdbConnection) {
        super(p);
        this.tsdbConnection = tsdbConnection;
    }

    @Override
    public Extractor getExtractor() {
        return new EmptyExtractor();
    }

    @Override
    public String getPath() {
        return p.getHost().getName() + "/" + p.getQualifiedName();
    }

    @Override
    public void commit(JrdsSample sample) {
        Map<String, String> probeTags = new HashMap<String, String>(2);
        probeTags.put("host", p.getHost().getName());
        probeTags.put("probe", p.getQualifiedName());
        Set<Deferred<Object>> deferreds = new HashSet<Deferred<Object>>(sample.size());
        for(Map.Entry<String, Number> i: sample.entrySet()) {
            Deferred<Object> d = tsdbConnection.addPoint(i.getKey(), sample.getTime().getTime(), i.getValue().longValue(), probeTags);
            deferreds.add(d);
        }
        for(Deferred<Object> d: deferreds) {
            try {
                d.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        tsdbConnection.flush();
    }

    @Override
    public Map<String, Number> getLastValues() {
        return Collections.emptyMap();
    }

    @Override
    public boolean checkStoreFile(ArchivesSet archives) {
        return true;
    }

    @Override
    public Date getLastUpdate() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public TSDB getStoreObject() {
        return tsdbConnection;
    }

    @Override
    public void closeStoreObject(Object object) {
        try {
            tsdbConnection.flush().join();
        } catch (HBaseException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
