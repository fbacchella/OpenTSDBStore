This package use the store functionality of [jrds](https://jrds.fr). All collected data will be sent to an OpenTSDB database and be displayed using [grafana](https://grafana.com).

To use it in jrds, add in jrds.properties:

    classpath=...;.../OpenTSDBStore/target/OpenTSDBStore-0.0.1-SNAPSHOT-jar-with-dependencies.jar
    stores=tsdb
    store.tsdb.factory=fr.jrds.opentsdbtore.OpentsdbStoreFactory
    store.tsdb.servers=server1;server2
    store.tsdb.port=4242
    
Usable settings:

 * port: the listening port of OpenTSDB servers, defaults to 4242.

 * publishers: how many publishing threads to create, defaults to 2.

 * timeout: the network timeout in seconds, defaults to 10s.

 * buffer: batch size, defaults to 20 samples.

 * servers: a semicolon separated list of servers to use, defaults to localhost. It can be an URI, to enforce HTTPS usage.
