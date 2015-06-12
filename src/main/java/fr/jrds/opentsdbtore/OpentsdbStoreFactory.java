package fr.jrds.opentsdbtore;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.hbase.async.Bytes;
import org.hbase.async.HBaseClient;

import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;
import jrds.Probe;
import jrds.PropertiesManager;
import jrds.store.AbstractStoreFactory;

public class OpentsdbStoreFactory extends AbstractStoreFactory<OpentsdbStore> {
    private TSDB tsdb = null;

    @Override
    public OpentsdbStore create(Probe<?, ?> p) {
        return new OpentsdbStore(p, tsdb);
    }

    @Override
    public void configureStore(PropertiesManager pm, Properties props) {
        super.configureStore(pm, props);
        //        try {
        //            hbaseInit(props);
        //        } catch (MasterNotRunningException e) {
        //            throw new RuntimeException("Failed to init hbase storage connector: " + e.getMessage(), e);
        //        } catch (ZooKeeperConnectionException e) {
        //            throw new RuntimeException("Failed to init hbase storage connector: " + e.getMessage(), e);
        //        } catch (IOException e) {
        //            Throwable rootCause = e;
        //            if(e.getCause() != null && e.getCause().getClass() == java.lang.reflect.InvocationTargetException.class) {
        //                rootCause = e.getCause().getCause();
        //            }
        //            throw new RuntimeException("Failed to init hbase storage connector: " + rootCause.getMessage(), rootCause);
        //        }


        try {
            Config tsdbConfig = new Config(false);
            System.out.println(tsdbConfig.dumpConfiguration());
            for(Map.Entry<Object, Object> i: props.entrySet()) {
                tsdbConfig.overrideConfig(i.getKey().toString(), i.getValue().toString());
            }
            tsdbConfig.overrideConfig("tsd.storage.hbase.zk_quorum", "rpmbuilder.prod.exalead.com");
            tsdb = new TSDB(tsdbConfig);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void hbaseInit(Properties props) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        Configuration hConf = HBaseConfiguration.create();
        hConf.set("hbase.zookeeper.quorum", props.getProperty("quorum", "rpmbuilder.prod.exalead.com"));
        //hConf.set("hbase.zookeeper.property.clientPort", props.getProperty("quorum", "rpmbuilder.prod.exalead.com"));
        hConf.setBoolean("hbase.defaults.for.version.skip", true);
        HBaseAdmin hBaseAdmin = new HBaseAdmin(hConf);

        hbaseInitTable(hBaseAdmin, "tsdb", "t");
        hbaseInitTable(hBaseAdmin, "tsdb-uid", "id", "name");
        hbaseInitTable(hBaseAdmin, "tsdb-tree", "t");
        hbaseInitTable(hBaseAdmin, "tsdb-meta", "name");

    }

    private void hbaseInitTable(HBaseAdmin hBaseAdmin, String tableName, String... columns) throws IOException {
        try {
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            if (!hBaseAdmin.tableExists(tableName)) {
                for (String s : columns) {
                    tableDescriptor.addFamily(new HColumnDescriptor(s));
                }
                hBaseAdmin.createTable(tableDescriptor);
            }
        } finally {
            hBaseAdmin.close();
        }
    }

}
