To use it in jrds, add in jrds.properties:

    classpath=...;.../OpenTSDBStore/target/OpenTSDBStore-0.0.1-SNAPSHOT-jar-with-dependencies.jar
    stores=tsdb
    store.tsdb.factory=fr.jrds.opentsdbtore.OpentsdbStoreFactory
    store.tsdb.tsd.storage.hbase.zk_quorum=...

