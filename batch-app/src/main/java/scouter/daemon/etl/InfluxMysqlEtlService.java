package scouter.daemon.etl;

import scouter.daemon.AppConfig;
import scouter.daemon.ConfigRef;

import javax.sql.DataSource;
import java.io.File;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class InfluxMysqlEtlService implements AutoCloseable {

    private static final ZoneId LOG_ZONE = ZoneId.of("Asia/Seoul");
    private static final DateTimeFormatter FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS VV");

    private final ConfigRef cfgRef;
    private final EtlLog log = EtlLog.of(InfluxMysqlEtlService.class);
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private volatile boolean started = false;

    private final InfluxReader influx;
    private final MySqlWriter mysql;
    private final InfluxQueryBuilder queryBuilder = new InfluxQueryBuilder();

    public InfluxMysqlEtlService(ConfigRef cfgRef, DataSource ds) {
        this.cfgRef = cfgRef;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "influx-rollup-sync");
            t.setDaemon(false);
            return t;
        });
        this.influx = new InfluxReader(cfgRef, log);
        this.mysql = new MySqlWriter(ds);
    }

    public void start() {
        if (started) {
            return;
        }
        started = true;

        AppConfig cfg = cfgRef.get();
        if (!getBool(cfg, "etl.enabled", true)) {
            log.info("[etl-rollup] disabled");
            return;
        }

        int intervalSec = getInt(cfg, "etl.interval.seconds", 30);

        scheduler.scheduleWithFixedDelay(() -> {
            if (!running.compareAndSet(false, true)) {
                log.warn("[etl-rollup] skipped (previous run still in progress)");
                return;
            }
            try {
                runOnce();
            } catch (Throwable t) {
                log.error("[etl-rollup] runOnce failed", t);
            } finally {
                running.set(false);
            }
        }, 2, Math.max(5, intervalSec), TimeUnit.SECONDS);

        log.info("[etl-rollup] started intervalSec=" + intervalSec);
    }

    private void runOnce() throws Exception {
        AppConfig cfg = cfgRef.get();
        if (!getBool(cfg, "etl.enabled", true)) {
            return;
        }

        List<JobSpec> jobs = JobSpec.load(cfg);
        if (jobs.isEmpty()) {
            log.warn("[etl-rollup] no jobs configured");
            return;
        }

        for (JobSpec job : jobs) {
            runJob(job);
        }
    }

    private void runJob(JobSpec job) throws Exception {
        Instant now = Instant.now().minusSeconds(Math.max(0, job.queryDelaySeconds));

        EtlStateStore stateStore = new EtlStateStore(new File(job.stateFile), log);

        Instant last = stateStore.loadLastInstant();
        if (last == null) {
            last = now.minusSeconds(job.lookbackMinutes * 60L);
        }

        Instant from = alignToWindow(last, job.windowSeconds, job.windowOffsetSeconds);
        Instant to = alignToWindow(now, job.windowSeconds, job.windowOffsetSeconds);

        if (!from.isBefore(to)) {
            return;
        }

        String influxQl = queryBuilder.buildQuery(
                job.measurement,
                job.fields,
                job.tags,
                from,
                to
        );

        InfluxResultSet rs = influx.query(influxQl);
        int written = mysql.upsertAll(job.mysqlTable, job.tags, job.fields, rs.rows());

        stateStore.saveLastInstant(to);

        String kstFrom = FMT.format(ZonedDateTime.ofInstant(from, LOG_ZONE));
        String kstTo = FMT.format(ZonedDateTime.ofInstant(to, LOG_ZONE));

        log.info("[etl-rollup:"+job.name+"] measurement="+job.measurement+
                " table="+job.mysqlTable+" from="+kstFrom+" to="+kstTo+" written="+written);
    }

    private Instant alignToWindow(Instant t, long windowSeconds, long offsetSeconds) {
        long epoch = t.getEpochSecond();
        long shifted = epoch - offsetSeconds;
        long floored = Math.floorDiv(shifted, windowSeconds) * windowSeconds;
        return Instant.ofEpochSecond(floored + offsetSeconds);
    }

    @Override
    public void close() {
        try {
            influx.close();
        } catch (Exception ignore) {
        }
        scheduler.shutdownNow();
        log.info("[etl-rollup] closed");
    }

    private static boolean getBool(AppConfig cfg, String key, boolean defVal) {
        String v = cfg.get(key, null);
        if (v == null) {
            return defVal;
        }
        return Boolean.parseBoolean(v.trim());
    }

    private static int getInt(AppConfig cfg, String key, int defVal) {
        String v = cfg.get(key, null);
        if (v == null || v.isBlank()) {
            return defVal;
        }
        try {
            return Integer.parseInt(v.trim());
        } catch (Exception e) {
            return defVal;
        }
    }

    private static long getLong(AppConfig cfg, String key, long defVal) {
        String v = cfg.get(key, null);
        if (v == null || v.isBlank()) {
            return defVal;
        }
        try {
            return Long.parseLong(v.trim());
        } catch (Exception e) {
            return defVal;
        }
    }

    private static final class JobSpec {
        final String name;
        final String measurement;
        final String mysqlTable;
        final String stateFile;
        final List<String> tags;
        final List<String> fields;
        final int lookbackMinutes;
        final int queryDelaySeconds;
        final long windowSeconds;
        final long windowOffsetSeconds;

        private JobSpec(
                String name,
                String measurement,
                String mysqlTable,
                String stateFile,
                List<String> tags,
                List<String> fields,
                int lookbackMinutes,
                int queryDelaySeconds,
                long windowSeconds,
                long windowOffsetSeconds
        ) {
            this.name = name;
            this.measurement = measurement;
            this.mysqlTable = mysqlTable;
            this.stateFile = stateFile;
            this.tags = tags;
            this.fields = fields;
            this.lookbackMinutes = lookbackMinutes;
            this.queryDelaySeconds = queryDelaySeconds;
            this.windowSeconds = windowSeconds;
            this.windowOffsetSeconds = windowOffsetSeconds;
        }

        static List<JobSpec> load(AppConfig cfg) {
            List<JobSpec> out = new ArrayList<>();

            List<String> jobs = InfluxQueryBuilder.splitCsv(cfg.get("etl.jobs", "1m,5m,1h,1d"));
            List<String> commonTags = InfluxQueryBuilder.splitCsv(
                    cfg.get("etl.tags", "obj,objFamily,objType,objHashTag")
            );
            List<String> commonFields = InfluxQueryBuilder.splitCsv(cfg.get("etl.fields", ""));

            for (String name : jobs) {
                String prefix = "etl.job." + name + ".";

                String measurement = cfg.get(prefix + "influx.measurement", "counter_" + name);
                String mysqlTable = cfg.get(prefix + "mysql.table", "scouter_counter_" + name);
                String stateFile = cfg.get(prefix + "state.file", "./data/etl_state_" + name + ".properties");

                List<String> tags = InfluxQueryBuilder.splitCsv(
                        cfg.get(prefix + "tags", String.join(",", commonTags))
                );
                List<String> fields = InfluxQueryBuilder.splitCsv(
                        cfg.get(prefix + "fields", String.join(",", commonFields))
                );

                int lookbackMinutes = getInt(cfg, prefix + "lookback.minutes", defaultLookbackMinutes(name));
                int queryDelaySeconds = getInt(cfg, prefix + "query.delay.seconds", defaultQueryDelaySeconds(name));

                long windowSeconds = getLong(cfg, prefix + "window.seconds", defaultWindowSeconds(name));
                long windowOffsetSeconds = getLong(cfg, prefix + "window.offset.seconds", defaultWindowOffsetSeconds(name));

                out.add(new JobSpec(
                        name,
                        measurement,
                        mysqlTable,
                        stateFile,
                        tags,
                        fields,
                        lookbackMinutes,
                        queryDelaySeconds,
                        windowSeconds,
                        windowOffsetSeconds
                ));
            }

            return out;
        }

        private static int defaultLookbackMinutes(String name) {
            if ("1d".equals(name)) {
                return 3 * 24 * 60;
            }
            if ("1h".equals(name)) {
                return 12 * 60;
            }
            if ("5m".equals(name)) {
                return 120;
            }
            return 30;
        }

        private static int defaultQueryDelaySeconds(String name) {
            if ("1d".equals(name)) {
                return 2 * 60 * 60;
            }
            if ("1h".equals(name)) {
                return 10 * 60;
            }
            if ("5m".equals(name)) {
                return 2 * 60;
            }
            return 90;
        }

        private static long defaultWindowSeconds(String name) {
            if ("1d".equals(name)) {
                return 86400L;
            }
            if ("1h".equals(name)) {
                return 3600L;
            }
            if ("5m".equals(name)) {
                return 300L;
            }
            return 60L;
        }

        private static long defaultWindowOffsetSeconds(String name) {
            if ("1d".equals(name)) {
                return -9L * 3600L;
            }
            return 0L;
        }
    }
}