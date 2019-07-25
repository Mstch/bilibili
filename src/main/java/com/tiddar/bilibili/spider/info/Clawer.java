package com.tiddar.bilibili.spider.info;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.squareup.okhttp.*;
import lombok.extern.log4j.Log4j2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;

@Log4j2
public class Clawer {

    private static String url = "https://api.bilibili.com/x/web-interface/view?aid=";
    // private static String[] proxys = {"144.123.71.12:9999", "1.197.203.124:9999", "121.63.199.213:9999", "106.42.173.178:9999", "171.15.51.148:9999", "125.109.198.11:9999", "218.91.112.66:9999", "1.194.39.14:9999", "182.34.16.120:9999", "60.2.44.182:30963", "218.91.112.59:9999", "115.223.108.232:8010", "117.90.6.129:9999", "113.121.22.220:9999", "113.128.9.114:9999", "58.253.159.222:9999", "180.119.68.34:9999", "114.230.117.82:9999", "163.204.92.131:9999", "111.177.113.193:9999", "1.197.16.251:9999", "163.204.244.91:9999", "113.124.92.219:9999", "58.254.220.116:52470", "113.120.37.50:9999", "113.124.92.49:9999", "212.64.51.13:8888", "27.43.191.102:9999", "120.83.123.108:9999", "120.83.101.216:9999", "58.253.152.16:9999", "119.33.64.3:8118", "171.11.178.74:9999", "163.204.94.42:9999", "123.8.123.179:9999", "118.187.58.34:53281", "1.198.72.151:9999", "115.231.5.230:44524", "180.118.247.69:9999", "210.22.176.146:32153", "113.121.21.221:9999", "111.75.223.9:35918", "222.89.32.142:48781", "49.86.178.161:9999", "1.194.145.150:9999", "112.111.217.199:9999", "106.42.190.178:9999", "58.253.152.22:9999", "1.197.11.130:9999", "171.80.113.3:9999", "124.93.201.59:59618", "117.91.133.18:9999", "42.238.88.35:9999", "163.204.92.66:9999", "113.121.23.110:9999", "120.83.98.54:9999", "115.219.106.125:8010", "82.114.241.138:8080", "113.124.85.154:9999", "113.110.44.242:9999", "113.121.23.188:9999", "182.34.37.62:9999", "120.83.102.155:9999", "112.250.29.31:8118", "111.224.167.252:8118", "117.91.253.137:9999", "1.198.109.162:9999", "222.189.144.163:9999", "171.80.113.109:9999", "27.208.231.7:8060", "144.123.71.23:9999", "117.64.149.134:808", "182.34.36.20:9999", "120.84.100.240:9999", "123.160.97.131:9999", "27.43.191.118:9999", "222.72.166.235:53281", "120.83.123.56:9999", "171.13.102.113:9999", "163.204.92.184:9999", "223.240.209.132:8010", "59.32.37.220:8010", "1.198.73.205:9999", "182.34.27.80:9999", "117.91.249.130:9999", "218.75.69.50:39590", "175.44.108.164:9999", "115.238.59.86:53400", "1.194.148.86:9999", "171.11.179.175:9999", "106.14.11.65:8118", "111.226.211.75:8118", "123.52.43.64:8118", "113.87.200.35:8118", "113.117.120.207:9999", "113.121.21.79:9999", "120.83.104.211:9999", "121.233.227.180:9999", "218.63.76.41:60485", "222.189.191.231:9999", "163.204.247.217:9999", "58.212.14.5:8118", "1.197.11.105:9999", "106.15.42.179:33543", "123.161.23.50:9797", "113.121.23.206:9999"};
    private static String[] proxys = {"27.208.231.7:8060", "212.64.51.13:8888", "47.94.200.124:3128", "82.114.241.138:8080"};
    private static int len = proxys.length;
    private static CopyOnWriteArrayList<Integer> list = new CopyOnWriteArrayList<>();

    public static void doProcess(List<Integer> aids, ExecutorService executor) throws InterruptedException {
        for (Integer aid : aids) {
            CompletableFuture.runAsync(() -> {
                        try {
                            ThreadLocalRandom random = ThreadLocalRandom.current();
                            String proxyStr = proxys[random.nextInt(len)];
                            InetSocketAddress address = new InetSocketAddress(proxyStr.split(":")[0], Integer.valueOf(proxyStr.split(":")[1]));
                            Proxy proxy = new Proxy(Proxy.Type.HTTP, address);
                            OkHttpClient client = new OkHttpClient();
                            client.setProxy(proxy);
                            Call call = client.newCall(new Request.Builder().get().url(url + aid).addHeader("Referer", "https://www.bilibili.com/video/av" + aid).addHeader("Origin", "https://www.bilibili.com").addHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36").build());
                            call.enqueue(new Callback() {
                                @Override
                                public void onFailure(Request request, IOException e) {
                                }
                                @Override
                                public void onResponse(Response response) throws IOException {
                                    Info info = null;
                                    info = Info.fromJson(aid, response.body().string());
                                    if (info != null) {
                                        Pusher.push(info);
                                    }
                                }
                            });
                        } catch (Exception e) {
                        }
                    }
                    , executor);
        }
    }


}
