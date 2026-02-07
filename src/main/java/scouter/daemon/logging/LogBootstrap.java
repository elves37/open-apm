package scouter.daemon.logging;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.*;

public final class LogBootstrap {

    private LogBootstrap() {}

    public static Closeable initAndScheduleCleanup(
            String logDir,
            String filePrefix,
            String timezone,
            boolean append,
            int retentionDays,
            int cleanupHour
    ) {
        ZoneId zoneIdTmp;
        try {
            zoneIdTmp = ZoneId.of(timezone);
        } catch (Exception e) {
            zoneIdTmp = ZoneId.systemDefault();
        }
        final ZoneId zoneId = zoneIdTmp;

        try {
            DailyRollingFileOutputStream os = new DailyRollingFileOutputStream(logDir, filePrefix, zoneId, append);
            PrintStream ps = new PrintStream(os, true, StandardCharsets.UTF_8.name());
            System.setOut(ps);
            System.setErr(ps);

            System.out.println("[log] initialized dir=" + logDir + " prefix=" + filePrefix +
                    " tz=" + zoneId + " retentionDays=" + retentionDays + " cleanupHour=" + cleanupHour);

            if (retentionDays > 0) {
                ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor(r -> {
                    Thread t = new Thread(r, "log-cleanup");
                    t.setDaemon(true);
                    return t;
                });

                long initialDelayMs = computeInitialDelayMs(zoneId, cleanupHour);
                long periodMs = TimeUnit.DAYS.toMillis(1);

                ses.scheduleAtFixedRate(() -> {
                    try {
                        cleanupOldLogs(logDir, filePrefix, retentionDays, zoneId);
                    } catch (Exception e) {
                        System.err.println("[log] cleanup failed: " + e.getMessage());
                    }
                }, initialDelayMs, periodMs, TimeUnit.MILLISECONDS);

                return () -> {
                    try { ses.shutdownNow(); } catch (Exception ignore) {}
                    try { os.close(); } catch (Exception ignore) {}
                };
            }

            return os::close;

        } catch (Exception e) {
            System.err.println("[log] init failed: " + e.getMessage());
            e.printStackTrace(System.err);
            return () -> {};
        }
    }

    private static long computeInitialDelayMs(ZoneId zoneId, int cleanupHour) {
        int hour = cleanupHour;
        if (hour < 0) hour = 0;
        if (hour > 23) hour = 23;

        ZonedDateTime now = ZonedDateTime.now(zoneId);
        ZonedDateTime next = now.withHour(hour).withMinute(0).withSecond(0).withNano(0);

        if (!next.isAfter(now)) {
            next = next.plusDays(1);
        }

        return Duration.between(now, next).toMillis();
    }

    private static void cleanupOldLogs(String logDir, String filePrefix, int retentionDays, ZoneId zoneId) throws IOException {
        Path dir = Paths.get((logDir == null || logDir.trim().isEmpty()) ? "./logs" : logDir.trim());
        if (!Files.exists(dir) || !Files.isDirectory(dir)) {
            return;
        }

        LocalDate cutoff = LocalDate.now(zoneId).minusDays(retentionDays);
        String prefix = (filePrefix == null || filePrefix.trim().isEmpty()) ? "app" : filePrefix.trim();

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, prefix + "-*.log")) {
            int deleted = 0;
            for (Path p : stream) {
                LocalDate day = parseDayFromFilename(prefix, p.getFileName().toString());
                if (day == null) continue;

                if (day.isBefore(cutoff)) {
                    try {
                        Files.deleteIfExists(p);
                        deleted++;
                    } catch (Exception ignore) {
                    }
                }
            }
            if (deleted > 0) {
                System.out.println("[log] cleanup deleted=" + deleted + " cutoffDay=" + cutoff);
            }
        }
    }

    private static LocalDate parseDayFromFilename(String prefix, String filename) {
        // expected: prefix-yyyyMMdd.log
        // example : apm-daemon-20260108.log
        int start = prefix.length() + 1; // '-'
        if (!filename.startsWith(prefix + "-")) return null;
        if (!filename.endsWith(".log")) return null;

        String core = filename.substring(0, filename.length() - 4);
        if (core.length() < start + 8) return null;

        String dayStr = core.substring(start, start + 8);
        try {
            return LocalDate.parse(dayStr, DateTimeFormatter.BASIC_ISO_DATE);
        } catch (Exception e) {
            return null;
        }
    }

    static final class DailyRollingFileOutputStream extends OutputStream {
        private static final DateTimeFormatter DAY = DateTimeFormatter.BASIC_ISO_DATE;

        private final String dir;
        private final String prefix;
        private final ZoneId zoneId;
        private final boolean append;

        private String currentDay;
        private OutputStream delegate;

        DailyRollingFileOutputStream(String dir, String prefix, ZoneId zoneId, boolean append) {
            this.dir = (dir == null || dir.trim().isEmpty()) ? "./logs" : dir.trim();
            this.prefix = (prefix == null || prefix.trim().isEmpty()) ? "app" : prefix.trim();
            this.zoneId = zoneId;
            this.append = append;
        }

        @Override
        public void write(int b) throws IOException {
            ensureOpen();
            delegate.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            ensureOpen();
            delegate.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            if (delegate != null) delegate.flush();
        }

        @Override
        public void close() throws IOException {
            if (delegate != null) delegate.close();
        }

        private void ensureOpen() throws IOException {
            String day = LocalDate.now(zoneId).format(DAY);

            if (delegate != null && day.equals(currentDay)) {
                return;
            }

            synchronized (this) {
                String day2 = LocalDate.now(zoneId).format(DAY);
                if (delegate != null && day2.equals(currentDay)) return;

                if (delegate != null) {
                    try { delegate.flush(); } catch (Exception ignore) {}
                    try { delegate.close(); } catch (Exception ignore) {}
                    delegate = null;
                }

                File d = new File(dir);
                if (!d.exists()) {
                    if (!d.mkdirs()) {
                        throw new IOException("Failed to create log dir: " + d.getAbsolutePath());
                    }
                }

                String filename = prefix + "-" + day2 + ".log";
                File f = new File(d, filename);
                delegate = new BufferedOutputStream(new FileOutputStream(f, append), 64 * 1024);
                currentDay = day2;
            }
        }
    }
}