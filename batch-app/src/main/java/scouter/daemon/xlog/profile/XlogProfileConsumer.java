package scouter.daemon.xlog.profile;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scouter.daemon.AppConfig;
import scouter.daemon.ConfigRef;

public class XlogProfileConsumer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(XlogProfileConsumer.class);

    private final ConfigRef cfgRef;
    private final XlogProfileDao dao;
    private final ObjectMapper objectMapper;
    private final KafkaConsumerManager consumerManager;
    private final PendingBuffer pendingBuffer = new PendingBuffer();
    private final RuntimeStats stats = new RuntimeStats();

    private volatile boolean running = true;
    private volatile long nextSummaryLogAtMs = 0L;
    private volatile long lastSummarySequence = -1L;

    public XlogProfileConsumer(ConfigRef cfgRef, XlogProfileDao dao) {
        this.cfgRef = cfgRef;
        this.dao = dao;
        this.objectMapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.consumerManager = new KafkaConsumerManager();
    }

    public void runLoop() {
        try {
            while (running) {
                AppConfig cfg = cfgRef.get();

                try {
                    if (!cfg.xlogProfileEnabled) {
                        flushIfBuffered(cfg, consumerManager.current(), "disabled");
                        consumerManager.close();
                        maybeLogSummary(cfg, System.currentTimeMillis(), true);
                        sleepMs(1000L);
                        continue;
                    }

                    if (consumerManager.requiresRecreate(cfg)) {
                        flushIfBuffered(cfg, consumerManager.current(), "config-change");
                    }

                    KafkaConsumer<String, String> consumer = consumerManager.ensure(cfg);
                    if (consumer == null) {
                        sleepMs(500L);
                        continue;
                    }

                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(cfg.kafkaPollTimeoutMs));
                    long now = System.currentTimeMillis();

                    if (!records.isEmpty()) {
                        bufferRecords(records, now);
                    }

                    if (shouldFlush(cfg, now)) {
                        flushBuffered(cfg, consumer, flushReason(cfg, now));
                    }

                    maybeLogSummary(cfg, now, false);
                } catch (WakeupException e) {
                    if (running) {
                        stats.recordError("consumer wakeup: " + e.getMessage());
                        log.warn("[xlog-profile] consumer wakeup", e);
                    }
                } catch (Exception e) {
                    stats.recordError("consumer loop error: " + e.getMessage());
                    log.error("[xlog-profile] consumer loop error", e);
                    sleepMs(1000L);
                }
            }

            flushIfBuffered(cfgRef.get(), consumerManager.current(), "shutdown");
        } finally {
            consumerManager.close();
            maybeLogSummary(cfgRef.get(), System.currentTimeMillis(), true);
            log.info("[xlog-profile] consumer stopped");
        }
    }

    private void bufferRecords(ConsumerRecords<String, String> records, long now) {
        for (ConsumerRecord<String, String> record : records) {
            pendingBuffer.trackOffset(record, now);
            stats.recordPolled();

            String payload = record.value();
            try {
                XlogProfileMessage message = objectMapper.readValue(payload, XlogProfileMessage.class);
                validate(message);
                pendingBuffer.addValid(message, now);
                stats.recordAccepted(stepCount(message));
            } catch (Exception e) {
                pendingBuffer.addReject(new RejectRecord(
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        payload,
                        e.toString()
                ), now);
                stats.recordRejected(e.toString());
            }
        }
    }

    private boolean shouldFlush(AppConfig cfg, long now) {
        if (pendingBuffer.isEmpty()) {
            return false;
        }
        if (pendingBuffer.validMessages.size() >= cfg.xlogProfileMaxBufferedMessages) {
            return true;
        }
        if (pendingBuffer.bufferedSteps >= cfg.xlogProfileMaxBufferedSteps) {
            return true;
        }
        if (pendingBuffer.rejectRecords.size() >= cfg.xlogProfileMaxBufferedRejects) {
            return true;
        }
        return now - pendingBuffer.firstBufferedAtMs >= cfg.xlogProfileFlushIntervalMs;
    }

    private String flushReason(AppConfig cfg, long now) {
        if (pendingBuffer.validMessages.size() >= cfg.xlogProfileMaxBufferedMessages) {
            return "message-limit";
        }
        if (pendingBuffer.bufferedSteps >= cfg.xlogProfileMaxBufferedSteps) {
            return "step-limit";
        }
        if (pendingBuffer.rejectRecords.size() >= cfg.xlogProfileMaxBufferedRejects) {
            return "reject-limit";
        }
        if (now - pendingBuffer.firstBufferedAtMs >= cfg.xlogProfileFlushIntervalMs) {
            return "interval";
        }
        return "manual";
    }

    private void flushIfBuffered(AppConfig cfg, KafkaConsumer<String, String> consumer, String reason) {
        if (!pendingBuffer.isEmpty()) {
            try {
                flushBuffered(cfg, consumer, reason);
            } catch (Exception e) {
                stats.recordFlushError(reason, e);
                log.error("[xlog-profile] flush failed before consumer state change. reason={}", reason, e);
            }
        }
    }

    private void flushBuffered(AppConfig cfg, KafkaConsumer<String, String> consumer, String reason) throws Exception {
        if (pendingBuffer.isEmpty()) {
            return;
        }

        int validCount = pendingBuffer.validMessages.size();
        int rejectCount = pendingBuffer.rejectRecords.size();
        int stepCount = pendingBuffer.bufferedSteps;
        int partitionCount = pendingBuffer.commitOffsets.size();
        long flushStart = System.currentTimeMillis();

        try {
            if (!pendingBuffer.validMessages.isEmpty()) {
                dao.saveBatch(pendingBuffer.validMessages);
            }

            for (RejectRecord rejectRecord : pendingBuffer.rejectRecords) {
                dao.saveReject(
                        rejectRecord.topic,
                        rejectRecord.partition,
                        rejectRecord.offset,
                        rejectRecord.payloadJson,
                        rejectRecord.errorMessage
                );
            }

            if (consumer != null && !pendingBuffer.commitOffsets.isEmpty()) {
                consumer.commitSync(new HashMap<>(pendingBuffer.commitOffsets));
            }

            long elapsedMs = System.currentTimeMillis() - flushStart;
            stats.recordFlush(reason, validCount, rejectCount, stepCount, elapsedMs, partitionCount);

            if (cfg.xlogProfileLogSlowFlushMs > 0 && elapsedMs >= cfg.xlogProfileLogSlowFlushMs) {
                log.info(
                        "[xlog-profile] slow flush reason={} elapsedMs={} validMessages={} steps={} rejects={} partitions={} committed={}",
                        reason,
                        elapsedMs,
                        validCount,
                        stepCount,
                        rejectCount,
                        partitionCount,
                        consumer != null
                );
            }

            pendingBuffer.clear();
        } catch (Exception e) {
            stats.recordFlushError(reason, e);
            throw e;
        }
    }

    private void maybeLogSummary(AppConfig cfg, long now, boolean force) {
        if (!cfg.xlogProfileLogSummaryEnabled) {
            return;
        }

        if (!force && now < nextSummaryLogAtMs) {
            return;
        }

        long sequence = stats.sequence + pendingBuffer.sequence;
        if (!force && sequence == lastSummarySequence) {
            nextSummaryLogAtMs = now + cfg.xlogProfileLogSummaryIntervalMs;
            return;
        }

        lastSummarySequence = sequence;
        nextSummaryLogAtMs = now + cfg.xlogProfileLogSummaryIntervalMs;

        log.info(
                "[xlog-profile] summary polled={} acceptedMessages={} acceptedSteps={} rejected={} flushes={} flushErrors={} bufferedMessages={} bufferedSteps={} bufferedRejects={} lastFlushReason={} lastFlushMs={} lastError={}",
                stats.totalPolled,
                stats.totalAcceptedMessages,
                stats.totalAcceptedSteps,
                stats.totalRejected,
                stats.totalFlushes,
                stats.totalFlushErrors,
                pendingBuffer.validMessages.size(),
                pendingBuffer.bufferedSteps,
                pendingBuffer.rejectRecords.size(),
                nullSafe(stats.lastFlushReason),
                stats.lastFlushElapsedMs,
                abbreviate(stats.lastErrorMessage, 160)
        );
    }

    private void validate(XlogProfileMessage message) {
        if (message == null) {
            throw new IllegalArgumentException("message is null");
        }
        if (message.getTime() == null) {
            throw new IllegalArgumentException("time is null");
        }
        if (message.getObjHash() == null) {
            throw new IllegalArgumentException("objHash is null");
        }
        if (message.getServiceHash() == null) {
            throw new IllegalArgumentException("serviceHash is null");
        }
        if (message.getTxid() == null || message.getTxid().trim().isEmpty()) {
            throw new IllegalArgumentException("txid is empty");
        }
        if (message.getDate() == null || message.getDate().trim().isEmpty()) {
            throw new IllegalArgumentException("date is empty");
        }
    }

    private static int stepCount(XlogProfileMessage message) {
        if (message == null || message.getSteps() == null) {
            return 0;
        }
        return message.getSteps().size();
    }

    private static String nullSafe(String value) {
        return value == null ? "" : value;
    }

    private static String abbreviate(String value, int maxLen) {
        if (value == null) {
            return "";
        }
        if (value.length() <= maxLen) {
            return value;
        }
        return value.substring(0, Math.max(0, maxLen - 3)) + "...";
    }

    @Override
    public void close() {
        running = false;
        consumerManager.wakeup();
    }

    private static void sleepMs(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static final class RejectRecord {
        private final String topic;
        private final int partition;
        private final long offset;
        private final String payloadJson;
        private final String errorMessage;

        private RejectRecord(String topic, int partition, long offset, String payloadJson, String errorMessage) {
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
            this.payloadJson = payloadJson;
            this.errorMessage = errorMessage;
        }
    }

    private static final class PendingBuffer {
        private final List<XlogProfileMessage> validMessages = new ArrayList<>();
        private final List<RejectRecord> rejectRecords = new ArrayList<>();
        private final Map<TopicPartition, OffsetAndMetadata> commitOffsets = new HashMap<>();
        private long firstBufferedAtMs = 0L;
        private int bufferedSteps = 0;
        private long sequence = 0L;

        private void trackOffset(ConsumerRecord<String, String> record, long now) {
            if (firstBufferedAtMs == 0L) {
                firstBufferedAtMs = now;
            }
            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
            long nextOffset = record.offset() + 1L;
            OffsetAndMetadata existing = commitOffsets.get(tp);
            if (existing == null || nextOffset > existing.offset()) {
                commitOffsets.put(tp, new OffsetAndMetadata(nextOffset));
            }
            sequence++;
        }

        private void addValid(XlogProfileMessage message, long now) {
            if (firstBufferedAtMs == 0L) {
                firstBufferedAtMs = now;
            }
            validMessages.add(message);
            bufferedSteps += stepCount(message);
            sequence++;
        }

        private void addReject(RejectRecord rejectRecord, long now) {
            if (firstBufferedAtMs == 0L) {
                firstBufferedAtMs = now;
            }
            rejectRecords.add(rejectRecord);
            sequence++;
        }

        private boolean isEmpty() {
            return validMessages.isEmpty() && rejectRecords.isEmpty() && commitOffsets.isEmpty();
        }

        private void clear() {
            validMessages.clear();
            rejectRecords.clear();
            commitOffsets.clear();
            firstBufferedAtMs = 0L;
            bufferedSteps = 0;
            sequence++;
        }
    }

    private static final class RuntimeStats {
        private long totalPolled = 0L;
        private long totalAcceptedMessages = 0L;
        private long totalAcceptedSteps = 0L;
        private long totalRejected = 0L;
        private long totalFlushes = 0L;
        private long totalFlushErrors = 0L;
        private String lastFlushReason = "";
        private long lastFlushElapsedMs = 0L;
        private String lastErrorMessage = "";
        private long sequence = 0L;

        private void recordPolled() {
            totalPolled++;
            sequence++;
        }

        private void recordAccepted(int steps) {
            totalAcceptedMessages++;
            totalAcceptedSteps += Math.max(0, steps);
            sequence++;
        }

        private void recordRejected(String errorMessage) {
            totalRejected++;
            lastErrorMessage = errorMessage;
            sequence++;
        }

        private void recordFlush(String reason, int validCount, int rejectCount, int stepCount, long elapsedMs, int partitionCount) {
            totalFlushes++;
            lastFlushReason = reason + "/partitions=" + partitionCount + "/valid=" + validCount + "/reject=" + rejectCount + "/steps=" + stepCount;
            lastFlushElapsedMs = elapsedMs;
            sequence++;
        }

        private void recordFlushError(String reason, Exception e) {
            totalFlushErrors++;
            lastFlushReason = reason;
            lastErrorMessage = e == null ? "" : e.toString();
            sequence++;
        }

        private void recordError(String message) {
            lastErrorMessage = message;
            sequence++;
        }
    }

    final class KafkaConsumerManager {

        private volatile KafkaConsumer<String, String> consumer;
        private volatile String consumerSig = "";

        boolean requiresRecreate(AppConfig cfg) {
            KafkaConsumer<String, String> current = consumer;
            return current != null && !cfg.xlogProfileKafkaConsumerSignature().equals(consumerSig);
        }

        KafkaConsumer<String, String> current() {
            return consumer;
        }

        KafkaConsumer<String, String> ensure(AppConfig cfg) {
            String sig = cfg.xlogProfileKafkaConsumerSignature();
            KafkaConsumer<String, String> current = consumer;
            if (current != null && sig.equals(consumerSig)) {
                return current;
            }

            close();

            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.kafkaBootstrap);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, cfg.xlogProfileKafkaGroupId);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, cfg.kafkaAutoOffsetReset);
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(cfg.kafkaMaxPollRecords));

            if (cfg.xlogProfileKafkaClientId != null && !cfg.xlogProfileKafkaClientId.isBlank()) {
                props.put(ConsumerConfig.CLIENT_ID_CONFIG, cfg.xlogProfileKafkaClientId);
            }

            KafkaConsumer<String, String> created = new KafkaConsumer<>(props);
            created.subscribe(Collections.singletonList(cfg.xlogProfileKafkaTopic), new RebalanceHandler(created));

            consumer = created;
            consumerSig = sig;

            log.info("[xlog-profile] consumer created topic={} groupId={} pollTimeoutMs={} flushIntervalMs={} maxPollRecords={}",
                    cfg.xlogProfileKafkaTopic,
                    cfg.xlogProfileKafkaGroupId,
                    cfg.kafkaPollTimeoutMs,
                    cfg.xlogProfileFlushIntervalMs,
                    cfg.kafkaMaxPollRecords);
            return created;
        }

        void wakeup() {
            KafkaConsumer<String, String> current = consumer;
            if (current != null) {
                try {
                    current.wakeup();
                } catch (Exception ignore) {
                }
            }
        }

        void close() {
            KafkaConsumer<String, String> current = consumer;
            consumer = null;
            consumerSig = "";
            if (current != null) {
                try {
                    current.close(Duration.ofSeconds(3));
                } catch (Exception ignore) {
                }
            }
        }
    }

    final class RebalanceHandler implements ConsumerRebalanceListener {

        private final KafkaConsumer<String, String> owner;

        RebalanceHandler(KafkaConsumer<String, String> owner) {
            this.owner = owner;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            if (!pendingBuffer.isEmpty()) {
                try {
                    flushBuffered(cfgRef.get(), owner, "rebalance-revoke");
                } catch (Exception e) {
                    stats.recordFlushError("rebalance-revoke", e);
                    log.error("[xlog-profile] flush failed on rebalance revoke. partitions={}", partitions, e);
                }
            }
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            if (!partitions.isEmpty()) {
                log.info("[xlog-profile] partitions assigned count={}", partitions.size());
            }
        }
    }
}
