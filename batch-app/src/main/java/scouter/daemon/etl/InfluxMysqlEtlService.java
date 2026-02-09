package scouter.daemon.etl;

import scouter.daemon.AppConfig;
import scouter.daemon.ConfigRef;

import javax.sql.DataSource;
import java.io.File;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class InfluxMysqlEtlService implements AutoCloseable {

    private final ConfigRef cfgRef;
    // private final DataSource ds;
    private final EtlLog log = EtlLog.of(InfluxMysqlEtlService.class);

    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private volatile boolean started = false;

    private volatile EtlStateStore stateStore;
    private final InfluxReader influx;
    private final MySqlWriter mysql;


    private static final ZoneId LOG_ZONE = ZoneId.of("Asia/Seoul");
    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS VV");
 
    public InfluxMysqlEtlService(ConfigRef cfgRef, DataSource ds) {
        this.cfgRef = cfgRef;
        // this.ds = ds;

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "influx-mysql-etl");
            t.setDaemon(false);
            return t;
        });

        AppConfig cfg = cfgRef.get();
        this.stateStore = new EtlStateStore(new File(get(cfg, "etl.state.file", "./config/etl_state_raw.properties")), log);

        this.influx = new InfluxReader(cfgRef, log);
        this.mysql = new MySqlWriter(ds, cfgRef);
    }

    public void start() {
        if (started) return;
        started = true;

        AppConfig cfg = cfgRef.get();
        if (!getBool(cfg, "etl.enabled", false)) {
            log.info("[etl-raw] disabled");
            return;
        }

        int intervalSec = getInt(cfg, "etl.interval.seconds", 60);

        scheduler.scheduleWithFixedDelay(() -> {
            if (!running.compareAndSet(false, true)) {
                log.warn("[etl-raw] skipped (previous run still in progress)");
                return;
            }
            try {
                runOnce();
            } catch (Throwable t) {
                log.error("[etl-raw] runOnce failed", t);
            } finally {
                running.set(false);
            }
        }, 2, Math.max(5, intervalSec), TimeUnit.SECONDS);

        log.info("[etl-raw] started intervalSec=" + intervalSec);
    }

    private void runOnce() throws Exception {
        AppConfig cfg = cfgRef.get();
        if (!getBool(cfg, "etl.enabled", false)) return;

        refreshStateStoreIfChanged(cfg);

        String measurement = get(cfg, "etl.influx.measurement", "counter");
        int lookbackMin = getInt(cfg, "etl.lookback.minutes", 10);
        int delaySec = getInt(cfg, "etl.query.delay.seconds", 15);

        Instant now = Instant.now().minusSeconds(Math.max(0, delaySec));
        Instant last = stateStore.loadLastInstant();

        if (last == null) {
            last = now.minusSeconds(lookbackMin * 60L);
        }

        Instant from = truncateToMinute(last);
        Instant to = truncateToMinute(now);
        if (!from.isBefore(to)) return;

        InfluxQueryBuilder qb = new InfluxQueryBuilder(cfg);
        String influxQl = qb.buildQuery(measurement, from, to);

        InfluxResultSet rs = influx.query(influxQl);

        int written = mysql.upsertAll(rs.rows());
        String kstFrom = FMT.format(ZonedDateTime.ofInstant(from, LOG_ZONE));
        String kstTo = FMT.format(ZonedDateTime.ofInstant(to, LOG_ZONE));
        log.info("[etl-raw] from=" + kstFrom + " to=" + kstTo + " written=" + written);

        stateStore.saveLastInstant(to);
    }

    private void refreshStateStoreIfChanged(AppConfig cfg) {
        String path = get(cfg, "etl.state.file", "./data/etl_state.properties");
        File f = new File(path);
        EtlStateStore cur = this.stateStore;
        if (!cur.isSameFile(f)) {
            this.stateStore = new EtlStateStore(f, log);
            log.info("[etl] state file changed: " + f.getPath());
        }
    }

    private Instant truncateToMinute(Instant t) {
        ZonedDateTime z = ZonedDateTime.ofInstant(t, ZoneOffset.UTC);
        ZonedDateTime m = z.withSecond(0).withNano(0);
        return m.toInstant();
    }

    @Override
    public void close() {
        try { influx.close(); } catch (Exception ignore) {}
        scheduler.shutdownNow();
        log.info("[etl] closed");
    }

    private static String get(AppConfig cfg, String key, String defVal) {
        String v = cfg.get(key, null);
        return (v == null || v.isBlank()) ? defVal : v.trim();
    }

    private static boolean getBool(AppConfig cfg, String key, boolean defVal) {
        String v = cfg.get(key, null);
        if (v == null) return defVal;
        return Boolean.parseBoolean(v.trim());
    }

    private static int getInt(AppConfig cfg, String key, int defVal) {
        String v = cfg.get(key, null);
        if (v == null) return defVal;
        try { return Integer.parseInt(v.trim()); } catch (Exception e) { return defVal; }
    }
}