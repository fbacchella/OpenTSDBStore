package fr.jrds.opentsdbtore;

import java.util.Collections;
import java.util.Date;
import java.util.Map;

import jrds.ArchivesSet;
import jrds.JrdsSample;
import jrds.Probe;
import jrds.store.AbstractStore;
import jrds.store.EmptyExtractor;
import jrds.store.Extractor;

public class OpentsdbStore extends AbstractStore<OpenTsdbConnexion> {

    private final OpenTsdbConnexion tsdbConnection;

    public OpentsdbStore(Probe<?, ?> p, OpenTsdbConnexion tsdbConnection) {
        super(p);
        this.tsdbConnection = tsdbConnection;
    }

    @Override
    public Extractor getExtractor() {
        return new EmptyExtractor();
    }

    @Override
    public String getPath() {
        return tsdbConnection.getPrefix(p);
    }

    @Override
    public void commit(JrdsSample sample) {
        tsdbConnection.newSample(sample);
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
        return null;
    }

    @Override
    public OpenTsdbConnexion getStoreObject() {
        return tsdbConnection;
    }

    @Override
    public void closeStoreObject(Object object) {
        tsdbConnection.flush();
    }

}
