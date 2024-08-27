package com.serviceMetrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;

import static com.codahale.metrics.MetricRegistry.name;
import static com.serviceMetrics.GetStarted.metrics;


/**
 * 1. 提供对于基础数据结构的监控指标
 * 2. 指标类型
 *    Meters
 *    Gauges
 *    Counters
 *    Histograms
 *    Timers
 *    Health Checks
 *
 * 3. reporter 种类
 * jmx
 *
 */
public class QueueManager {
    private final Queue queue;
    private final Counter pendingJobs = metrics.counter(name(QueueManager.class, "pending-jobs"));

    public QueueManager(MetricRegistry metrics, String name) {
        this.queue = new ConcurrentLinkedDeque();
        metrics.register(name(QueueManager.class, name, "size"),
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return queue.size();
                    }
                });
    }

//    public void addJob(Job job) {
//        pendingJobs.inc();
//        queue.offer(job);
//    }
//
//    public Job takeJob() {
//        pendingJobs.dec();
//        return queue.take();
//    }
}
