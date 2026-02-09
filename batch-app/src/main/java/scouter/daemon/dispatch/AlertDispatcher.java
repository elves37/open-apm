package scouter.daemon.dispatch;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import scouter.daemon.AppConfig;
import scouter.daemon.ConfigRef;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlertDispatcher implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(AlertDispatcher.class);
    
    private final ConfigRef cfgRef;
    private final KafkaConsumerManager consumerManager;
    private final WorkerPool workerPool;
    private final AlertHandler handler;

    private volatile boolean running = true;

    public AlertDispatcher(ConfigRef cfgRef, ObjectMapper om, SubscriptionDao dao, int initialThreads) {
        this.cfgRef = cfgRef;
        this.consumerManager = new KafkaConsumerManager();
        this.workerPool = new WorkerPool(Math.max(1, initialThreads));
        this.handler = new AlertHandler(cfgRef, om, dao);
    }

    public void runLoop() {
        while (running) {
            AppConfig cfg = cfgRef.get();

            if (!cfg.dispatchEnabled) {
                consumerManager.close();
                sleepMs(1000);
                continue;
            }

            workerPool.resize(cfg.dispatchWorkerThreads);
            KafkaConsumer<String, String> c = consumerManager.ensure(cfg);

            if (c == null) {
                sleepMs(500);
                continue;
            }

            try {
                ConsumerRecords<String, String> records = c.poll(Duration.ofMillis(cfg.kafkaPollTimeoutMs));
                if (records.isEmpty()) continue;

                List<Future<Boolean>> futures = new ArrayList<>(records.count());
                for (ConsumerRecord<String, String> r : records) {
                    futures.add(workerPool.submit(() -> handler.handle(r)));
                }

                boolean ok = true;
                for (Future<Boolean> f : futures) {
                    try {
                        if (!Boolean.TRUE.equals(f.get())) ok = false;
                    } catch (Exception e) {
                        ok = false;
                        System.err.println("[dispatch] worker error: " + e);
                    }
                }

                if (ok) {
                    c.commitSync();
                } else {
                    log.info("[dispatch] worker failure: skip commitSync");
                }
            } catch (WakeupException we) {
                // consumer 교체/종료 시 발생 가능
            } catch (Exception e) {
                System.err.println("[dispatch] loop error: " + e);
                sleepMs(1000);
            }
        }

        consumerManager.close();
    }

    @Override
    public void close() {
        running = false;
        consumerManager.close();
        workerPool.shutdownNow();
    }

    private static void sleepMs(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    static final class KafkaConsumerManager {
        private volatile KafkaConsumer<String, String> consumer;
        private volatile String consumerSig = "";

        KafkaConsumer<String, String> ensure(AppConfig cfg) {
            String sig = cfg.kafkaConsumerSignature();
            KafkaConsumer<String, String> c = consumer;
            if (c != null && sig.equals(consumerSig)) return c;

            close();

            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.kafkaBootstrap);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, cfg.kafkaGroupId);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(cfg.kafkaEnableAutoCommit));
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, cfg.kafkaAutoOffsetReset);
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(cfg.kafkaMaxPollRecords));

            KafkaConsumer<String, String> created = new KafkaConsumer<>(props);
            created.subscribe(Collections.singletonList(cfg.kafkaTopicAlert));

            consumer = created;
            consumerSig = sig;

            log.info("[kafka] consumer created topic=" + cfg.kafkaTopicAlert + " sig=" + sig);
            return created;
        }

        void close() {
            KafkaConsumer<String, String> c = consumer;
            consumer = null;
            consumerSig = "";

            if (c != null) {
                try { c.wakeup(); } catch (Exception ignore) {}
                try { c.close(Duration.ofSeconds(3)); } catch (Exception ignore) {}
                log.info("[kafka] consumer closed");
            }
        }
    }

    static final class WorkerPool {
        private final ThreadPoolExecutor executor;

        WorkerPool(int threads) {
            int n = Math.max(1, threads);
            this.executor = new ThreadPoolExecutor(
                    n, n,
                    60, TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>(),
                    r -> {
                        Thread t = new Thread(r, "dispatch-worker");
                        t.setDaemon(false);
                        return t;
                    }
            );
            this.executor.allowCoreThreadTimeOut(true);
        }

        void resize(int desiredThreads) {
            int n = Math.max(1, desiredThreads);
            if (executor.getCorePoolSize() == n && executor.getMaximumPoolSize() == n) return;

            executor.setCorePoolSize(n);
            executor.setMaximumPoolSize(n);
            log.info("[dispatch] workerThreads changed to " + n);
        }

        Future<Boolean> submit(java.util.concurrent.Callable<Boolean> task) {
            return executor.submit(task);
        }

        void shutdownNow() {
            executor.shutdownNow();
        }
    }
}