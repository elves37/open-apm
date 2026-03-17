package scouter.daemon;

import java.io.FileInputStream;
import java.util.Objects;
import java.util.Properties;

public class AppConfig {

    private final Properties p;
    private final String configPath;

    public final String scouterApiUrl;
    public final String scouterAuthHeader;
    public final int httpTimeoutSeconds;

    public final String dbMysqlJdbcUrl;
    public final String dbMysqlUsername;
    public final String dbMysqlPassword;
    public final int dbMysqlPoolSize;
    public final int dbMysqlMinimumIdle;
    public final int dbMysqlConnectionTimeoutMs;
    public final int dbMysqlIdleTimeoutMs;
    public final int dbMysqlMaxLifetimeSeconds;
    public final boolean dbMysqlRewriteBatchedStatements;
    public final boolean dbMysqlCachePrepStmts;
    public final int dbMysqlPrepStmtCacheSize;
    public final int dbMysqlPrepStmtCacheSqlLimit;

    public final boolean syncEnabled;
    public final int syncIntervalSeconds;
    public final int inactiveGraceMinutes;
    public final int batchSize;
    public final boolean syncMarkInactiveEnabled;

    public final boolean dispatchEnabled;
    public final String kafkaBootstrap;
    public final String kafkaGroupId;
    public final String kafkaTopicAlert;
    public final String kafkaAutoOffsetReset;
    public final boolean kafkaEnableAutoCommit;
    public final int kafkaMaxPollRecords;
    public final int kafkaPollTimeoutMs;
    public final int dispatchWorkerThreads;

    public final boolean xlogProfileEnabled;
    public final String xlogProfileKafkaGroupId;
    public final String xlogProfileKafkaTopic;
    public final String xlogProfileKafkaClientId;
    public final boolean xlogProfileStoreRawBase64;
    public final boolean xlogProfileStorePayloadJson;
    public final boolean xlogProfileStoreStepRawJson;
    public final int xlogProfileHeaderBatchSize;
    public final int xlogProfileStepBatchSize;
    public final int xlogProfileFlushIntervalMs;
    public final int xlogProfileMaxBufferedMessages;
    public final int xlogProfileMaxBufferedSteps;
    public final int xlogProfileMaxBufferedRejects;
    public final boolean xlogProfileLogSummaryEnabled;
    public final int xlogProfileLogSummaryIntervalMs;
    public final int xlogProfileLogSlowFlushMs;
    public final String xlogProfileMysqlHeaderTable;
    public final String xlogProfileMysqlStepTable;
    public final String xlogProfileMysqlRejectTable;

    public final String hipushIgnoreNamePatterns;
    public final String hipushIgnoreTitlePatterns;
    public final String hipushIgnoreMessagePatterns;
    public final boolean hipushIgnoreContinuousDupAlert;

    public final boolean smsEnabled;
    public final String cisInternalSysId;
    public final String cisInternalURL;

    public final String etlEnabled;
    public final String etlIntervalSeconds;
    public final String etlLookbackMinutes;
    public final String etlQueryDelaySeconds;
    public final String etlInfluxUrl;
    public final String etlInfluxUser;
    public final String etlInfluxPass;
    public final String etlInfluxDb;
    public final String etlInfluxMeasurement;
    public final String etlTags;
    public final String etlFields;
    public final String etlFieldAgg;
    public final String etlMysqlTable;
    public final String etlStateFile;

    private AppConfig(Properties p, String configPath) {
        this.p = p;
        this.configPath = configPath;

        this.scouterApiUrl = get("scouter.api.url", "http://127.0.0.1:6188/scouter/v1/object");
        this.scouterAuthHeader = get("scouter.auth.header", "");
        this.httpTimeoutSeconds = getInt("http.timeout.seconds", 10);

        this.dbMysqlJdbcUrl = mustAny("db.mysql.jdbcUrl", "db.jdbcUrl");
        this.dbMysqlUsername = getAny("root", "db.mysql.username", "db.username");
        this.dbMysqlPassword = getAny("", "db.mysql.password", "db.password");
        this.dbMysqlPoolSize = getIntAny(10, "db.mysql.poolSize", "db.poolSize");
        this.dbMysqlMinimumIdle = getInt("db.mysql.minimumIdle", 1);
        this.dbMysqlConnectionTimeoutMs = getInt("db.mysql.connectionTimeout.ms", 10_000);
        this.dbMysqlIdleTimeoutMs = getInt("db.mysql.idleTimeout.ms", 60_000);
        this.dbMysqlMaxLifetimeSeconds = getInt("db.mysql.maxLifetime.seconds", 10) * 60_000;
        this.dbMysqlRewriteBatchedStatements = getBool("db.mysql.rewriteBatchedStatements", true);
        this.dbMysqlCachePrepStmts = getBool("db.mysql.cachePrepStmts", true);
        this.dbMysqlPrepStmtCacheSize = getInt("db.mysql.prepStmtCacheSize", 250);
        this.dbMysqlPrepStmtCacheSqlLimit = getInt("db.mysql.prepStmtCacheSqlLimit", 2048);

        this.syncEnabled = getBool("sync.enabled", false);
        this.syncIntervalSeconds = getInt("sync.interval.seconds", 60);
        this.inactiveGraceMinutes = getInt("inactive.grace.minutes", 1440);
        this.batchSize = getInt("batch.size", 500);
        this.syncMarkInactiveEnabled = getBool("sync.mark.inactive.enabled", true);

        this.dispatchEnabled = getBool("dispatch.enabled", false);
        this.kafkaBootstrap = must("kafka.bootstrap.servers");
        this.kafkaGroupId = get("kafka.group.id", "apm-alert-dispatcher");
        this.kafkaTopicAlert = get("kafka.topic.alert", "apm.alert");
        this.kafkaAutoOffsetReset = get("kafka.auto.offset.reset", "earliest");
        this.kafkaEnableAutoCommit = getBool("kafka.enable.auto.commit", false);
        this.kafkaMaxPollRecords = getInt("kafka.max.poll.records", 500);
        this.kafkaPollTimeoutMs = getInt("kafka.poll.timeout.ms", 1000);
        this.dispatchWorkerThreads = getInt("dispatch.workerThreads", 4);

        this.xlogProfileEnabled = getBool("xlog.profile.enabled", false);
        this.xlogProfileKafkaGroupId = get("xlog.profile.kafka.group.id", "apm-xlog-profile-consumer");
        this.xlogProfileKafkaTopic = get("xlog.profile.kafka.topic", "scouter.xlog.profile");
        this.xlogProfileKafkaClientId = get("xlog.profile.kafka.client.id", "apm-xlog-profile-consumer");
        this.xlogProfileStoreRawBase64 = getBool("xlog.profile.store.raw.base64", false);
        this.xlogProfileStorePayloadJson = getBool("xlog.profile.store.payload.json", false);
        this.xlogProfileStoreStepRawJson = getBool("xlog.profile.store.step.raw.json", false);
        this.xlogProfileHeaderBatchSize = getInt("xlog.profile.mysql.header.batch.size", 500);
        this.xlogProfileStepBatchSize = getInt("xlog.profile.mysql.step.batch.size", 5000);

        int xlogFlushMs = getInt("xlog.profile.flush.interval.ms", -1);
        if (xlogFlushMs <= 0) {
            xlogFlushMs = getInt("xlog.profile.flush.interval.seconds", 1) * 1000;
        }
        this.xlogProfileFlushIntervalMs = Math.max(1000, xlogFlushMs);
        this.xlogProfileMaxBufferedMessages = Math.max(1, getInt("xlog.profile.max.buffered.messages", 5000));
        this.xlogProfileMaxBufferedSteps = Math.max(1, getInt("xlog.profile.max.buffered.steps", 100000));
        this.xlogProfileMaxBufferedRejects = Math.max(1, getInt("xlog.profile.max.buffered.rejects", 1000));
        this.xlogProfileLogSummaryEnabled = getBool("xlog.profile.log.summary.enabled", true);

        int xlogSummaryMs = getInt("xlog.profile.log.summary.interval.ms", -1);
        if (xlogSummaryMs <= 0) {
            xlogSummaryMs = getInt("xlog.profile.log.summary.interval.seconds", 300) * 1000;
        }
        this.xlogProfileLogSummaryIntervalMs = Math.max(1000, xlogSummaryMs);
        this.xlogProfileLogSlowFlushMs = Math.max(0, getInt("xlog.profile.log.slow.flush.ms", 5000));

        this.xlogProfileMysqlHeaderTable = get("xlog.profile.mysql.header.table", "scouter_xlog_profile");
        this.xlogProfileMysqlStepTable = get("xlog.profile.mysql.step.table", "scouter_xlog_profile_step");
        this.xlogProfileMysqlRejectTable = get("xlog.profile.mysql.reject.table", "scouter_xlog_profile_reject");

        this.smsEnabled = getBool("dispatch.sms.enabled", false);
        this.hipushIgnoreNamePatterns = get("ext_plugin_hipush_ignore_name_patterns", "");
        this.hipushIgnoreTitlePatterns = get("ext_plugin_hipush_ignore_title_patterns", "");
        this.hipushIgnoreMessagePatterns = get("ext_plugin_hipush_ignore_message_patterns", "");
        this.hipushIgnoreContinuousDupAlert = getBool("ext_plugin_hipush_ignore_continuous_dup_alert", false);
        this.cisInternalSysId = get("sys.SysId", "");
        this.cisInternalURL = get("sys.sURL", "");

        this.etlEnabled = get("etl.enabled", "false");
        this.etlIntervalSeconds = get("etl.interval.seconds", "60");
        this.etlLookbackMinutes = get("etl.lookback.minutes", "10");
        this.etlQueryDelaySeconds = get("etl.query.delay.seconds", "15");
        this.etlInfluxUrl = get("etl.influx.url", "http://127.0.0.1:8086");
        this.etlInfluxUser = get("etl.influx.user", "");
        this.etlInfluxPass = get("etl.influx.pass", "");
        this.etlInfluxDb = get("etl.influx.db", "scouter");
        this.etlInfluxMeasurement = get("etl.influx.measurement", "counter");
        this.etlTags = get("etl.tags", "obj,objFamily,objType");
        this.etlFields = get(
                "etl.fields",
                "ActiveService,ApiErrorRate,ApiTPS,ApiTime,ApiTimeByService,CpuTime,Elapsed90%,ElapsedTime,ErrorRate,GcCount,GcTime,HeapTotal,HeapUsed,PermPercent,PermUsed,QueuingTime,RecentUser,ServiceCount,SqlErrorRate,SqlTPS,SqlTime,SqlTimeByService,TPS,objHash"
        );
        this.etlFieldAgg = get(
                "etl.fieldAgg",
                "CpuTime=max,ElapsedTime=max,Elapsed90%=max,GcCount=max,GcTime=max,ActiveService=max,RecentUser=max,ServiceCount=max,objHash=max"
        );
        this.etlMysqlTable = get("etl.mysql.table", "scouter_counter_1m");
        this.etlStateFile = get("etl.state.file", "./data/etl_state.properties");
    }

    public static AppConfig load() {
        String path = System.getProperty("config.file", "config/application.properties");
        return load(path);
    }

    public static AppConfig load(String path) {
        Properties p = new Properties();
        try (FileInputStream fis = new FileInputStream(path)) {
            p.load(fis);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load properties: " + path + " (" + e.getMessage() + ")", e);
        }
        return new AppConfig(p, path);
    }

    public String get(String key, String def) {
        String v = p.getProperty(key);
        return (v == null || v.trim().isEmpty()) ? def : v.trim();
    }

    public String getAny(String def, String... keys) {
        for (String key : keys) {
            String v = p.getProperty(key);
            if (v != null && !v.trim().isEmpty()) {
                return v.trim();
            }
        }
        return def;
    }

    public int getInt(String key, int def) {
        String v = p.getProperty(key);
        if (v == null || v.trim().isEmpty()) {
            return def;
        }
        try {
            return Integer.parseInt(v.trim());
        } catch (Exception e) {
            return def;
        }
    }

    public int getIntAny(int def, String... keys) {
        for (String key : keys) {
            String v = p.getProperty(key);
            if (v == null || v.trim().isEmpty()) {
                continue;
            }
            try {
                return Integer.parseInt(v.trim());
            } catch (Exception ignore) {
            }
        }
        return def;
    }

    public boolean getBool(String key, boolean def) {
        String v = p.getProperty(key);
        if (v == null || v.trim().isEmpty()) {
            return def;
        }
        return Boolean.parseBoolean(v.trim());
    }

    public String must(String key) {
        String v = p.getProperty(key);
        if (v == null || v.trim().isEmpty()) {
            throw new IllegalArgumentException("Missing property: " + key);
        }
        return v.trim();
    }

    public String mustAny(String... keys) {
        for (String key : keys) {
            String v = p.getProperty(key);
            if (v != null && !v.trim().isEmpty()) {
                return v.trim();
            }
        }
        throw new IllegalArgumentException("Missing property: " + String.join(" or ", keys));
    }

    public Properties raw() {
        return p;
    }

    public String summary() {
        return "AppConfig{path='" + configPath +
                "', syncEnabled=" + syncEnabled +
                ", syncIntervalSeconds=" + syncIntervalSeconds +
                ", dispatchEnabled=" + dispatchEnabled +
                ", topic='" + kafkaTopicAlert + "'" +
                ", xlogProfileEnabled=" + xlogProfileEnabled +
                ", xlogProfileTopic='" + xlogProfileKafkaTopic + "'" +
                ", xlogProfileFlushIntervalMs=" + xlogProfileFlushIntervalMs +
                ", workerThreads=" + dispatchWorkerThreads +
                ", smsEnabled=" + smsEnabled +
                "}";
    }

    public String kafkaConsumerSignature() {
        return kafkaBootstrap + "|" + kafkaGroupId + "|" + kafkaTopicAlert + "|" +
                kafkaAutoOffsetReset + "|" + kafkaEnableAutoCommit + "|" + kafkaMaxPollRecords;
    }

    public String xlogProfileKafkaConsumerSignature() {
        return kafkaBootstrap + "|" + xlogProfileKafkaGroupId + "|" + xlogProfileKafkaTopic + "|" +
                xlogProfileKafkaClientId + "|" + kafkaAutoOffsetReset + "|" + kafkaMaxPollRecords;
    }

    public String syncSignature() {
        return syncEnabled + "|" + syncIntervalSeconds + "|" + inactiveGraceMinutes + "|" +
                batchSize + "|" + syncMarkInactiveEnabled;
    }

    @Override
    public String toString() {
        return summary();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AppConfig)) {
            return false;
        }
        AppConfig other = (AppConfig) o;
        return Objects.equals(this.summary(), other.summary());
    }

    @Override
    public int hashCode() {
        return Objects.hash(summary());
    }
}
