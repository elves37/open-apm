package scouter.daemon.dispatch;

import scouter.daemon.AppConfig;
import scouter.daemon.dispatch.SubscriptionDao.DispatchContext;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class AlertPreprocessor {

    private final Deduper deduper = new Deduper();

    public PreparedAlert preprocess(AppConfig cfg, AlertEvent ev, DispatchContext ctx) {
        String name = (ctx.objName == null || ctx.objName.trim().isEmpty()) ? "N/A" : ctx.objName.trim();
        String title = (ev.title == null) ? "" : ev.title;
        String msg = (ev.message == null) ? "" : ev.message;

        if ((name.equals("N/A") || name.isEmpty()) && msg.endsWith("connected.")) {
            int idx = msg.indexOf("connected");
            if (idx > -1) {
                if (msg.contains("reconnected")) {
                    int end = idx - 6;
                    if (end > 0 && end <= msg.length()) name = msg.substring(0, end).trim();
                } else {
                    int end = idx - 4;
                    if (end > 0 && end <= msg.length()) name = msg.substring(0, end).trim();
                }
            }
        }

        if ("INACTIVE_OBJECT".equals(title)) {
            title = "An object has been inactivated.";
            int cut = msg.indexOf("OBJECT");
            if (cut > 1) msg = msg.substring(0, cut - 1);
        }

        if (matchesAnyWildcardCsv(name, cfg.hipushIgnoreNamePatterns)) return null;
        if (matchesAnyWildcardCsv(title, cfg.hipushIgnoreTitlePatterns)) return null;
        if (matchesAnyMessagePattern(msg, cfg.hipushIgnoreMessagePatterns)) return null;

        if (cfg.hipushIgnoreContinuousDupAlert) {
            long now = System.currentTimeMillis();
            if (!deduper.allow(ev.objHash, ev.title, now, TimeUnit.HOURS.toMillis(1))) return null;
        }

        long occurredAt = (ev.occurredAt == null) ? System.currentTimeMillis() : ev.occurredAt;
        String severity = normalizeSeverity(ev.severity);

        String contents = formatTime(occurredAt) + " [" + severity + "] " + title + ": " + msg;
        return new PreparedAlert(name, title, msg, contents, occurredAt, severity);
    }

    static String truncate(String s, int max) {
        if (s == null) return "";
        return s.length() <= max ? s : s.substring(0, max) + "...";
    }

    private static String formatTime(long millis) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss");
        return sdf.format(new Date(millis));
    }

    private static String normalizeSeverity(String s) {
        if (s == null || s.trim().isEmpty()) return "INFO";
        String v = s.trim();

        if (isAllDigits(v)) {
            int n;
            try { n = Integer.parseInt(v); } catch (Exception e) { return "INFO"; }
            switch (n) {
                case 0: return "INFO";
                case 1: return "WARN";
                case 2: return "ERROR";
                case 3: return "FATAL";
                default: return v;
            }
        }

        return v.toUpperCase(Locale.ROOT);
    }

    private static boolean isAllDigits(String s) {
        if (s == null || s.isEmpty()) return false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c < '0' || c > '9') return false;
        }
        return true;
    }

    private static boolean matchesAnyWildcardCsv(String value, String csv) {
        if (value == null) value = "";
        if (csv == null || csv.trim().isEmpty()) return false;

        String[] patterns = csv.split(",");
        for (String raw : patterns) {
            String p = raw.trim();
            if (p.isEmpty()) continue;

            String regex = p.replaceAll("\\*", ".*");
            if (value.matches(regex)) return true;
        }
        return false;
    }

    private static boolean matchesAnyMessagePattern(String msg, String csv) {
        if (msg == null) msg = "";
        if (csv == null || csv.trim().isEmpty()) return false;

        String[] patterns = csv.split(",");
        for (String raw : patterns) {
            String p = raw.trim();
            if (p.isEmpty()) continue;

            String regex = p.replaceAll("\\*", ".*")
                    .replaceAll("\\(", "\\\\(").replaceAll("\\)", "\\\\)")
                    .replaceAll("\\[", "\\\\[").replaceAll("\\]", "\\\\]");
            if (msg.matches(regex)) return true;
        }
        return false;
    }

    static final class Deduper {
        private static final class Key {
            final String objHash;
            final String rawTitle;

            Key(String objHash, String rawTitle) {
                this.objHash = objHash == null ? "" : objHash;
                this.rawTitle = rawTitle == null ? "" : rawTitle;
            }

            @Override
            public int hashCode() {
                int h = 17;
                h = 31 * h + objHash.hashCode();
                h = 31 * h + rawTitle.hashCode();
                return h;
            }

            @Override
            public boolean equals(Object o) {
                if (!(o instanceof Key)) return false;
                Key other = (Key) o;
                return objHash.equals(other.objHash) && rawTitle.equals(other.rawTitle);
            }
        }

        private final ConcurrentHashMap<Key, Long> lastSent = new ConcurrentHashMap<>();

        boolean allow(String objHash, String rawTitle, long nowMs, long windowMs) {
            if (windowMs <= 0) return true;

            Key k = new Key(objHash, rawTitle);
            Long prev = lastSent.get(k);
            if (prev != null && (nowMs - prev) < windowMs) {
                return false;
            }

            lastSent.put(k, nowMs);

            if (lastSent.size() > 200_000) {
                purgeOld(nowMs, windowMs);
            }

            return true;
        }

        private void purgeOld(long nowMs, long windowMs) {
            for (var e : lastSent.entrySet()) {
                Long ts = e.getValue();
                if (ts == null) continue;
                if ((nowMs - ts) >= windowMs) lastSent.remove(e.getKey(), ts);
            }
        }
    }
}