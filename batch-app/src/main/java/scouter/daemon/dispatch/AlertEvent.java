package scouter.daemon.dispatch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class AlertEvent {
    public final String objHash;
    public final String severity;
    public final String title;
    public final String message;
    public final Long occurredAt;

    public AlertEvent(String objHash, String severity, String title, String message, Long occurredAt) {
        this.objHash = objHash;
        this.severity = severity;
        this.title = title;
        this.message = message;
        this.occurredAt = occurredAt;
    }

    public static AlertEvent parse(ObjectMapper om, String json) throws Exception {
        JsonNode n = om.readTree(json);
        String objHash = firstText(n, "objHash", "obj_hash", "objectKey", "object_key");
        String severity = firstText(n, "severity", "level", "alarmLevel");
        String title = firstText(n, "title", "name");
        String message = firstText(n, "message", "msg", "detail");
        Long occurredAt = firstLong(n, "occurredAt", "occurred_at", "time", "timestamp");
        return new AlertEvent(objHash, severity, title, message, occurredAt);
    }

    private static String firstText(JsonNode n, String... keys) {
        for (String k : keys) {
            JsonNode v = n.get(k);
            if (v != null && !v.isNull()) {
                String s = v.asText();
                if (s != null && !s.trim().isEmpty()) return s.trim();
            }
        }
        return null;
    }

    private static Long firstLong(JsonNode n, String... keys) {
        for (String k : keys) {
            JsonNode v = n.get(k);
            if (v == null || v.isNull()) continue;
            if (v.isNumber()) return v.asLong();
            String s = v.asText();
            if (s == null || s.trim().isEmpty()) continue;
            try { return Long.parseLong(s.trim()); } catch (Exception ignore) {}
        }
        return null;
    }
}