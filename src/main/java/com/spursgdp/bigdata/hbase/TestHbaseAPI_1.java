package com.spursgdp.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @author zhangdongwei
 * @create 2020-05-11-9:51
 */
public class TestHbaseAPI_1 {

    public static void main(String[] args) throws IOException {

        //通过java代码访问MySQL数据库 JDBC
        //1) 加载数据库驱动
        //2) 获取数据库连接（url、username、password） Connection
        //3) 获取数据库操作对象  Statement、PrepareStatement
        //4) sql
        //5) 执行数据库操作
        //6) 获取查询结果ResultSet

        //通过java代码访问HBase数据库
        //0) 创建配置对象,加载配置文件
        // classsloader : 加载HBaseConfiguration类的classloader
        // classpath: hbase-default.xml hbase-site.xml
        Configuration conf = HBaseConfiguration.create();

        //1) 获取hbase连接对象: Connection
        Connection connection = ConnectionFactory.createConnection(conf);
        System.out.println(connection);


        //2) 获取操作对象: Admin
        Admin admin = connection.getAdmin();  //通过connection.get获取，类似于Statement

        //3) 操作数据库
        
        //3.1) 判断命名空间是否存在
        String namespace = "zhangdw";
        try {
            admin.getNamespaceDescriptor(namespace);
        }catch (NamespaceNotFoundException e ) {
            //3.2) 创建Namespace
            NamespaceDescriptor nd = NamespaceDescriptor.create(namespace).build();
            admin.createNamespace(nd);
//            throw new RuntimeException("Namespace " + namespace + "不存在...");
        }

        //3.2) 判断表是否存在
        TableName tableName = TableName.valueOf("zhangdw:student");
        boolean exists = admin.tableExists(tableName);
//        System.out.println(exists);
        if(!exists) {
            //3.3) 创建表
            HTableDescriptor td = new HTableDescriptor(tableName);  //创建表描述符
            HColumnDescriptor cd = new HColumnDescriptor("info");  //创建列族描述符
            td.addFamily(cd);
            admin.createTable(td);
        }


        //3.4) 查询表



        //4) 获取操作结果

        //5) 关闭数据库连接

    }
}
