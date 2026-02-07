package scouter.daemon.etl;

import java.io.*;
import java.time.Instant;
import java.util.Properties;

final class EtlStateStore {

    private final File file;
    private final EtlLog log;

    EtlStateStore(File file, EtlLog log) {
        this.file = file;
        this.log = log;
    }

    boolean isSameFile(File other) {
        try {
            return file.getCanonicalFile().equals(other.getCanonicalFile());
        } catch (Exception e) {
            return file.getPath().equals(other.getPath());
        }
    }

    Instant loadLastInstant() {
        if (!file.exists()) return null;

        Properties p = new Properties();
        try (InputStream in = new FileInputStream(file)) {
            p.load(in);
            String v = p.getProperty("lastInstant");
            if (v == null || v.isBlank()) return null;
            return Instant.parse(v.trim());
        } catch (Exception e) {
            log.warn("[etl] state load failed: " + e.getMessage());
            return null;
        }
    }

    void saveLastInstant(Instant t) {
        File parent = file.getParentFile();
        if (parent != null && !parent.exists()) parent.mkdirs();

        Properties p = new Properties();
        p.setProperty("lastInstant", t.toString());

        try (OutputStream out = new FileOutputStream(file)) {
            p.store(out, "etl state");
        } catch (Exception e) {
            log.warn("[etl] state save failed: " + e.getMessage());
        }
    }
}