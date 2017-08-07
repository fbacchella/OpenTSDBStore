To use it in jrds, add in jrds.properties:

    classpath=...;.../OpenTSDBStore/target/OpenTSDBStore-0.0.1-SNAPSHOT-jar-with-dependencies.jar
    stores=tsdb
    store.tsdb.factory=fr.jrds.opentsdbtore.OpentsdbStoreFactory
    store.tsdb.servers=server1;server2
    store.tsdb.port=4242
    
Usable settings:

 * port: The listnening port of OpenTSDB servers, default to 4242.

 * publishers: Who many publishing threads to create, default to 2.

 * timeout: the networks timeout in seconds, default to 10s.

 * buffer: batch size, default to 20 samples.

 * servers: a semi-column separated lists of servers to use, default to localhost. It can be an URI, to enforce HTTPS usage.

