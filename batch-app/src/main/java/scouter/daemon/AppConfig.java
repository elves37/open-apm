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

    public final String dbJdbcUrl;
    public final String dbUsername;
    public final String dbPassword;
    public final int dbPoolSize;

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

        this.dbJdbcUrl = must("db.jdbcUrl");
        this.dbUsername = get("db.username", "root");
        this.dbPassword = get("db.password", "");
        this.dbPoolSize = getInt("db.poolSize", 10);

        this.syncEnabled = getBool("sync.enabled", true);
        this.syncIntervalSeconds = getInt("sync.interval.seconds", 60);
        this.inactiveGraceMinutes = getInt("inactive.grace.minutes", 1440);
        this.batchSize = getInt("batch.size", 500);
        this.syncMarkInactiveEnabled = getBool("sync.mark.inactive.enabled", true);

        this.dispatchEnabled = getBool("dispatch.enabled", true);
        this.kafkaBootstrap = must("kafka.bootstrap.servers");
        this.kafkaGroupId = get("kafka.group.id", "apm-alert-dispatcher");
        this.kafkaTopicAlert = get("kafka.topic.alert", "apm.alert");
        this.kafkaAutoOffsetReset = get("kafka.auto.offset.reset", "earliest");
        this.kafkaEnableAutoCommit = getBool("kafka.enable.auto.commit", false);
        this.kafkaMaxPollRecords = getInt("kafka.max.poll.records", 200);
        this.kafkaPollTimeoutMs = getInt("kafka.poll.timeout.ms", 1000);

        this.dispatchWorkerThreads = getInt("dispatch.workerThreads", 4);
        this.smsEnabled = getBool("dispatch.sms.enabled", false);

        this.hipushIgnoreNamePatterns = get("ext_plugin_hipush_ignore_name_patterns", "");
        this.hipushIgnoreTitlePatterns = get("ext_plugin_hipush_ignore_title_patterns", "");
        this.hipushIgnoreMessagePatterns = get("ext_plugin_hipush_ignore_message_patterns", "");
        this.hipushIgnoreContinuousDupAlert = getBool("ext_plugin_hipush_ignore_continuous_dup_alert", false);
        
        this.cisInternalSysId = get("sys.SysId", "");
        this.cisInternalURL = get("sys.sURL", "");

        this.etlEnabled = get("etl.enabled","true");
        this.etlIntervalSeconds = get("etl.interval.seconds","60");
        this.etlLookbackMinutes = get("etl.lookback.minutes","10");
        this.etlQueryDelaySeconds = get("etl.query.delay.seconds","15");
        this.etlInfluxUrl = get("etl.influx.url","http://127.0.0.1:8086");
        this.etlInfluxUser = get("etl.influx.user","");
        this.etlInfluxPass = get("etl.influx.pass","");
        this.etlInfluxDb = get("etl.influx.db","scouter");
        this.etlInfluxMeasurement = get("etl.influx.measurement","counter");
        this.etlTags = get("etl.tags","obj,objFamily,objType");
        this.etlFields = get("etl.fields","ActiveService,ApiErrorRate,ApiTPS,ApiTime,ApiTimeByService,CpuTime,Elapsed90%,ElapsedTime,ErrorRate,GcCount,GcTime,HeapTotal,HeapUsed,PermPercent,PermUsed,QueuingTime,RecentUser,ServiceCount,SqlErrorRate,SqlTPS,SqlTime,SqlTimeByService,TPS,objHash");
        this.etlFieldAgg = get("etl.fieldAgg","CpuTime=max,ElapsedTime=max,Elapsed90%=max,GcCount=max,GcTime=max,ActiveService=max,RecentUser=max,ServiceCount=max,objHash=max");
        this.etlMysqlTable = get("etl.mysql.table","scouter_counter_1m");
        this.etlStateFile = get("etl.state.file","./data/etl_state.properties");
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

    public int getInt(String key, int def) {
        String v = p.getProperty(key);
        if (v == null || v.trim().isEmpty()) return def;
        try { return Integer.parseInt(v.trim()); } catch (Exception e) { return def; }
    }

    public boolean getBool(String key, boolean def) {
        String v = p.getProperty(key);
        if (v == null || v.trim().isEmpty()) return def;
        return Boolean.parseBoolean(v.trim());
    }

    public String must(String key) {
        String v = p.getProperty(key);
        if (v == null || v.trim().isEmpty()) throw new IllegalArgumentException("Missing property: " + key);
        return v.trim();
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
                ", workerThreads=" + dispatchWorkerThreads +
                ", smsEnabled=" + smsEnabled +
                "}";
    }

    // Kafka consumer를 재생성해야 하는지 판단하는 서명
    public String kafkaConsumerSignature() {
        return kafkaBootstrap + "|" + kafkaGroupId + "|" + kafkaTopicAlert + "|" +
                kafkaAutoOffsetReset + "|" + kafkaEnableAutoCommit + "|" + kafkaMaxPollRecords;
    }

    // Sync 쪽 실행 주기/옵션 서명
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
        if (!(o instanceof AppConfig)) return false;
        AppConfig other = (AppConfig) o;
        return Objects.equals(this.summary(), other.summary());
    }

    @Override
    public int hashCode() {
        return Objects.hash(summary());
    }
}