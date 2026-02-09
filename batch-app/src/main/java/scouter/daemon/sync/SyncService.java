package scouter.daemon.sync;

import scouter.daemon.AppConfig;
import scouter.daemon.ConfigRef;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

public final class SyncService implements AutoCloseable {
    private final ConfigRef cfgRef;
    private final ObjectSyncJob job;

    private volatile boolean running = true;
    private Thread thread;

    private static final DateTimeFormatter TS =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

    public SyncService(ConfigRef cfgRef, ObjectSyncJob job) {
        this.cfgRef = cfgRef;
        this.job = job;
    }

    public void start() {
        thread = new Thread(this::loop, "sync-service");
        thread.setDaemon(false);
        thread.start();
    }

    private void loop() {
        String lastSig = "";

        while (running) {
            AppConfig cfg = cfgRef.get();

            String sig = cfg.syncSignature();
            if (!sig.equals(lastSig)) {
                System.out.println("[sync] config changed: " + sig);
                lastSig = sig;
            }

            if (!cfg.syncEnabled) {
                // 꺼져있으면 1초마다 확인
                sleepSecondsInterruptible(1);
                continue;
            }

            // 한 번 실행
            job.runOnceSafe();

            int interval = normalizeInterval(cfg.syncIntervalSeconds);
            long nextRunAtMs = System.currentTimeMillis() + interval * 1000L;

            System.out.println("[sync] nextRunAt=" + TS.format(Instant.ofEpochMilli(nextRunAtMs))
                    + " (in " + interval + "s)");

            // interval 동안 1초씩 자면서 설정 변경 즉시 반영
            for (int i = 0; i < interval && running; i++) {
                AppConfig cur = cfgRef.get();

                // 중간에 sync를 꺼버리면 즉시 중단
                if (!cur.syncEnabled) {
                    System.out.println("[sync] disabled while waiting. stop sleep.");
                    break;
                }

                int curInterval = normalizeInterval(cur.syncIntervalSeconds);
                if (curInterval != interval) {
                    long newNextMs = System.currentTimeMillis() + curInterval * 1000L;
                    System.out.println("[sync] interval changed while waiting: " + interval + " -> " + curInterval
                            + " newNextRunAt=" + TS.format(Instant.ofEpochMilli(newNextMs)));
                    break;
                }

                if (!sleepSecondsInterruptible(1)) break;
            }
        }
    }

    private int normalizeInterval(int v) {
        return (v <= 0) ? 60 : v;
    }

    private boolean sleepSecondsInterruptible(int seconds) {
        try {
            TimeUnit.SECONDS.sleep(seconds);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    @Override
    public void close() {
        running = false;
        if (thread != null) thread.interrupt();
    }
}