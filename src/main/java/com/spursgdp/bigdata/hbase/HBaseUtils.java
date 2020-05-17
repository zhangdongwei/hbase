package com.spursgdp.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @author zhangdongwei
 * @create 2020-05-11-18:12
 */
public class HBaseUtils {

    static ThreadLocal<Connection> connHolder = new ThreadLocal<Connection>();

    public static void makeConnection() throws IOException {
        if(connHolder.get() == null){
            Configuration conf = HBaseConfiguration.create();
            //TODO 这里后续改为从连接池获取连接
            connHolder.set(ConnectionFactory.createConnection(conf));
        }
    }

    public static void closeConnection() throws IOException {
        if(connHolder.get() != null){
            connHolder.get().close();
            connHolder.remove();
        }
    }

    //TODO 其他HBase操作，获取connection都通过connHolder.get()方式获取...
}
