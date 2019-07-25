package com.tiddar.bilibili;

import com.tiddar.bilibili.spider.info.Clawer;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class Main {

    public static void main(String[] args) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(100);
        List<Integer> aids = new ArrayList<>(1000000);
        for (int i = 20000; i < 10000000; i++) {
            aids.add(i + 1);
        }
        Clawer.doProcess(aids, executor);
    }
}
