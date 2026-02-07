package scouter.daemon;

import java.nio.file.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.file.StandardWatchEventKinds.*;

public final class ConfigReloader implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(BatchApp.class);
    private final Path configFile;
    private final ConfigRef cfgRef;

    private final ScheduledExecutorService exec;
    private volatile boolean running = true;
    private final AtomicLong lastReloadAtMs = new AtomicLong(0);

    private volatile Thread watcherThread;

    public ConfigReloader(Path configFile, ConfigRef cfgRef) {
        this.configFile = configFile.toAbsolutePath().normalize();
        this.cfgRef = cfgRef;
        this.exec = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "config-reloader");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        Thread t = new Thread(this::watchLoop, "config-watcher");
        t.setDaemon(true);
        this.watcherThread = t;
        t.start();
    }

    private void watchLoop() {
        Path dir = configFile.getParent();
        if (dir == null) {
            log.error("[config] invalid config path: {}", configFile);
            return;
        }
        try (WatchService ws = FileSystems.getDefault().newWatchService()) {
            dir.register(ws, ENTRY_CREATE, ENTRY_MODIFY, ENTRY_DELETE);

            log.info("[config] watching " + configFile);

            while (running) {
                WatchKey key = ws.take();
                for (WatchEvent<?> ev : key.pollEvents()) {
                    Object ctx = ev.context();
                    if (!(ctx instanceof Path)) continue;
                    Path changed = dir.resolve((Path) ctx).toAbsolutePath().normalize();
                    if (!changed.equals(configFile)) continue;
                    scheduleReloadDebounced();
                }
                key.reset();
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("[config] watcher stopped: {}", e.getMessage());
        }
    }

    private void scheduleReloadDebounced() {
        long now = System.currentTimeMillis();
        long prev = lastReloadAtMs.getAndSet(now);

        // 일부 편집기는 ENTRY_MODIFY를 연속으로 내보내므로 300ms 디바운스
        if (now - prev < 300) return;

        exec.schedule(() -> {
            try {
                AppConfig next = AppConfig.load(configFile.toString());
                cfgRef.set(next);
                log.info("[config] reloaded: {}", next.summary());
            } catch (Exception e) {
                log.error("[config] reload failed: {}", e.getMessage());
            }
        }, 300, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        running = false;

        Thread wt = watcherThread;
        if (wt != null) wt.interrupt();

        exec.shutdownNow();
    }
}