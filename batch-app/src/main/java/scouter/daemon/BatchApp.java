package scouter.daemon;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.util.TimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scouter.daemon.dispatch.AlertDispatcher;
import scouter.daemon.dispatch.SubscriptionDao;
import scouter.daemon.etl.InfluxMysqlEtlService;
import scouter.daemon.logging.LogBootstrap;
import scouter.daemon.sync.ObjectSyncJob;
import scouter.daemon.sync.ScouterClient;
import scouter.daemon.sync.SyncService;
import scouter.daemon.xlog.profile.XlogProfileConsumer;
import scouter.daemon.xlog.profile.XlogProfileDao;

public class BatchApp {

    public static void main(String[] args) {
        System.out.println(Instant.now().minusSeconds(Math.max(0, 15)));

        String configPath = System.getProperty("config.file", "config/application.properties");
        AppConfig initial = AppConfig.load(configPath);
        configureRuntimeLogging(initial);
        ConfigRef cfgRef = new ConfigRef(initial);

        LogBootstrap.initAndScheduleCleanup(
                initial.get("log.dir", "./logs"),
                initial.get("log.file.prefix", "apm-daemon"),
                initial.get("log.timezone", "Asia/Seoul"),
                initial.getBool("log.append", true),
                initial.getInt("log.retention.days", 30),
                initial.getInt("log.cleanup.hour", 3)
        );

        Logger log = LoggerFactory.getLogger(BatchApp.class);
        log.info("[boot] {}", initial.summary());

        ConfigReloader reloader = new ConfigReloader(Paths.get(configPath), cfgRef);
        reloader.start();

        HikariDataSource ds = new HikariDataSource(hikari(cfgRef.get()));
        ObjectMapper om = new ObjectMapper();

        ScouterClient scouterClient = new ScouterClient(cfgRef.get());
        ObjectSyncJob syncJob = new ObjectSyncJob(cfgRef, ds, scouterClient);
        SyncService syncService = new SyncService(cfgRef, syncJob);
        syncService.start();

        SubscriptionDao dao = new SubscriptionDao(ds);
        AlertDispatcher dispatcher = new AlertDispatcher(cfgRef, om, dao, cfgRef.get().dispatchWorkerThreads);
        Thread dispatchThread = new Thread(dispatcher::runLoop, "kafka-dispatcher");
        dispatchThread.setDaemon(false);
        dispatchThread.start();

        XlogProfileDao xlogProfileDao = new XlogProfileDao(
                ds,
                om,
                cfgRef.get().xlogProfileMysqlHeaderTable,
                cfgRef.get().xlogProfileMysqlStepTable,
                cfgRef.get().xlogProfileMysqlRejectTable,
                cfgRef.get().xlogProfileStoreRawBase64,
                cfgRef.get().xlogProfileStorePayloadJson,
                cfgRef.get().xlogProfileStoreStepRawJson,
                cfgRef.get().xlogProfileHeaderBatchSize,
                cfgRef.get().xlogProfileStepBatchSize
        );
        XlogProfileConsumer xlogProfileConsumer = new XlogProfileConsumer(cfgRef, xlogProfileDao);
        Thread xlogProfileThread = new Thread(xlogProfileConsumer::runLoop, "kafka-xlog-profile-consumer");
        xlogProfileThread.setDaemon(false);
        xlogProfileThread.start();

        InfluxMysqlEtlService influxMysqlEtlService = new InfluxMysqlEtlService(cfgRef, ds);
        influxMysqlEtlService.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("[shutdown] ...");
            try {
                reloader.close();
            } catch (Exception ignore) {
            }
            try {
                syncService.close();
            } catch (Exception ignore) {
            }
            try {
                dispatcher.close();
            } catch (Exception ignore) {
            }
            try {
                xlogProfileConsumer.close();
            } catch (Exception ignore) {
            }
            try {
                xlogProfileThread.join(5000L);
            } catch (Exception ignore) {
            }
            try {
                influxMysqlEtlService.close();
            } catch (Exception ignore) {
            }
            try {
                ds.close();
            } catch (Exception ignore) {
            }
            log.info("[shutdown] done.");
        }));
    }

    private static void configureRuntimeLogging(AppConfig cfg) {
        String timezone = cfg.get("log.timezone", "Asia/Seoul");
        try {
            TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of(timezone)));
        } catch (Exception ignore) {
        }

        setIfAbsent("org.slf4j.simpleLogger.showDateTime", cfg.get("log.showDateTime", "true"));
        setIfAbsent("org.slf4j.simpleLogger.dateTimeFormat", cfg.get("log.dateTimeFormat", "yyyy-MM-dd HH:mm:ss.SSS XXX"));
        setIfAbsent("org.slf4j.simpleLogger.showThreadName", cfg.get("log.showThreadName", "true"));
        setIfAbsent("org.slf4j.simpleLogger.showLogName", cfg.get("log.showLogName", "true"));
        setIfAbsent("org.slf4j.simpleLogger.showShortLogName", cfg.get("log.showShortLogName", "false"));
        setIfAbsent("org.slf4j.simpleLogger.levelInBrackets", cfg.get("log.levelInBrackets", "true"));
    }

    private static void setIfAbsent(String key, String value) {
        if (System.getProperty(key) == null && value != null && !value.isBlank()) {
            System.setProperty(key, value);
        }
    }

    private static HikariConfig hikari(AppConfig cfg) {
        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl(cfg.dbMysqlJdbcUrl);
        hc.setUsername(cfg.dbMysqlUsername);
        hc.setPassword(cfg.dbMysqlPassword);
        hc.setMaximumPoolSize(cfg.dbMysqlPoolSize);
        hc.setMinimumIdle(cfg.dbMysqlMinimumIdle);
        hc.setConnectionTimeout(cfg.dbMysqlConnectionTimeoutMs);
        hc.setIdleTimeout(cfg.dbMysqlIdleTimeoutMs);
        hc.setMaxLifetime(cfg.dbMysqlMaxLifetimeSeconds);
        hc.addDataSourceProperty("rewriteBatchedStatements", String.valueOf(cfg.dbMysqlRewriteBatchedStatements));
        hc.addDataSourceProperty("cachePrepStmts", String.valueOf(cfg.dbMysqlCachePrepStmts));
        hc.addDataSourceProperty("prepStmtCacheSize", String.valueOf(cfg.dbMysqlPrepStmtCacheSize));
        hc.addDataSourceProperty("prepStmtCacheSqlLimit", String.valueOf(cfg.dbMysqlPrepStmtCacheSqlLimit));
        return hc;
    }
}
