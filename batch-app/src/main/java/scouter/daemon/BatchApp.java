package scouter.daemon;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import scouter.daemon.dispatch.AlertDispatcher;
import scouter.daemon.dispatch.SubscriptionDao;
import scouter.daemon.etl.InfluxMysqlEtlService;
import scouter.daemon.logging.LogBootstrap;
import scouter.daemon.sync.ObjectSyncJob;
import scouter.daemon.sync.ScouterClient;
import scouter.daemon.sync.SyncService;

import java.nio.file.Paths;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchApp {
    public static void main(String[] args) {
        System.out.println(Instant.now().minusSeconds(Math.max(0, 15)));
        String configPath = System.getProperty("config.file", "config/application.properties");

        AppConfig initial = AppConfig.load(configPath);
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

        InfluxMysqlEtlService influxMysqlEtlService = new InfluxMysqlEtlService(cfgRef, ds);
        influxMysqlEtlService.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("[shutdown] ...");
            try { reloader.close(); } catch (Exception ignore) {}
            try { syncService.close(); } catch (Exception ignore) {}
            try { dispatcher.close(); } catch (Exception ignore) {}
            try { influxMysqlEtlService.close(); } catch (Exception ignore) {}
            try { ds.close(); } catch (Exception ignore) {}
            log.info("[shutdown] done.");
        }));
    }

    private static HikariConfig hikari(AppConfig cfg) {
        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl(cfg.dbJdbcUrl);
        hc.setUsername(cfg.dbUsername);
        hc.setPassword(cfg.dbPassword);
        hc.setMaximumPoolSize(cfg.dbPoolSize);
        hc.setMinimumIdle(1);
        hc.setConnectionTimeout(10_000);
        hc.setIdleTimeout(60_000);
        hc.setMaxLifetime(10 * 60_000);
        return hc;
    }
}