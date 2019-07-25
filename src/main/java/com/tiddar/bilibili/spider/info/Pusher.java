package com.tiddar.bilibili.spider.info;

import com.tiddar.bilibili.spider.info.Info;
import lombok.extern.log4j.Log4j2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Log4j2
public class Pusher {
    private static Stack<Connection> connections = new Stack<>();
    private static Info[] list = new Info[500];
    private static AtomicInteger index = new AtomicInteger(0);
    private static Configuration conf;

    private static Configuration getConf() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop0:2181,hadoop01:2181,hadoop02:2181");
        return conf;
    }

    public static Connection getConnection() throws IOException {
        if (connections.size() == 0) {
            synchronized (connections) {
                if (connections.size() == 0) {
                    for (int i = 0; i < 10; i++) {
                        if (conf == null) {
                            conf = getConf();
                        }
                        connections.push(ConnectionFactory.createConnection(conf));
                    }
                }
            }
        }
        while (connections.peek() != null) {
            return connections.pop();
        }
        return null;
    }

    public static void release(Connection connection) {
        connections.push(connection);
    }

    public static void push(Info info) throws IOException {
        try {
            list[index.getAndIncrement()] = info;
        } catch (IndexOutOfBoundsException e) {
            index.set(0);
            List<Put> putList = Arrays.stream(list).map(Info::toPut).collect(Collectors.toList());
            getConnection().getTable(TableName.valueOf("bilibili")).put(putList);
            list[index.getAndIncrement()] = info;
            log.debug("成功传入hbase" + info.getAid());
        }
    }


}
