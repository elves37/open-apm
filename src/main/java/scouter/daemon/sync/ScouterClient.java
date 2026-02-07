package scouter.daemon.sync;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import scouter.daemon.AppConfig;

import java.io.IOException;
import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class ScouterClient {
    private final AppConfig cfg;
    private final ObjectMapper om = new ObjectMapper();
    private final HttpClient http;

    public ScouterClient(AppConfig cfg) {
        this.cfg = cfg;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(cfg.httpTimeoutSeconds))
                .build();
    }

    public List<ScouterObject> fetchObjects() throws IOException, InterruptedException {
        HttpRequest.Builder rb = HttpRequest.newBuilder()
                .uri(URI.create(cfg.scouterApiUrl))
                .timeout(Duration.ofSeconds(cfg.httpTimeoutSeconds))
                .GET()
                .header("Accept", "application/json");

        if (!isBlank(cfg.scouterAuthHeader)) {
            rb.header("Authorization", cfg.scouterAuthHeader);
        }

        HttpResponse<String> res = http.send(rb.build(), HttpResponse.BodyHandlers.ofString());
        if (res.statusCode() / 100 != 2) {
            throw new IOException("Scouter API non-2xx: " + res.statusCode() + " body=" + truncate(res.body(), 500));
        }

        JsonNode root = om.readTree(res.body());
        JsonNode arr = null;
        if (root.isArray()) arr = root;
        else if (root.has("result") && root.get("result").isArray()) arr = root.get("result");
        else if (root.has("objects") && root.get("objects").isArray()) arr = root.get("objects");

        if (arr == null) throw new IOException("Unknown JSON shape: " + truncate(root.toString(), 500));

        List<ScouterObject> out = new ArrayList<>();
        for (JsonNode n : arr) out.add(map(n));
        return out;
    }

    private ScouterObject map(JsonNode n) throws IOException {
        String objHash = textRequired(n, "objHash");
        String objType = text(n, "objType");
        String objFamily = text(n, "objFamily");
        String objName = text(n, "objName");
        String address = text(n, "address");
        String version = text(n, "version");
        boolean alive = bool(n, "alive");
        Long lastWakeUpMs = longFromText(n, "lastWakeUpTime");
        String tagsJson = (n.has("tags") && !n.get("tags").isNull()) ? n.get("tags").toString() : null;

        return new ScouterObject(objHash, objType, objFamily, objName, address, version, alive, lastWakeUpMs, tagsJson);
    }

    private static String text(JsonNode n, String field) {
        JsonNode v = n.get(field);
        if (v == null || v.isNull()) return null;
        String s = v.asText();
        return isBlank(s) ? null : s;
    }

    private static String textRequired(JsonNode n, String field) throws IOException {
        String v = text(n, field);
        if (isBlank(v)) throw new IOException("Missing required field: " + field);
        return v;
    }

    private static boolean bool(JsonNode n, String field) {
        JsonNode v = n.get(field);
        if (v == null || v.isNull()) return false;
        return v.asBoolean(false);
    }

    private static Long longFromText(JsonNode n, String field) {
        String s = text(n, field);
        if (isBlank(s)) return null;
        try { return Long.parseLong(s); } catch (Exception e) { return null; }
    }

    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }

    private static String truncate(String s, int max) {
        if (s == null) return "";
        return s.length() <= max ? s : s.substring(0, max) + "...";
    }

    public static final class ScouterObject {
        public final String objHash;
        public final String objType;
        public final String objFamily;
        public final String objName;
        public final String address;
        public final String version;
        public final boolean alive;
        public final Long lastWakeUpMs;
        public final String tagsJson;

        public ScouterObject(String objHash, String objType, String objFamily, String objName,
                             String address, String version, boolean alive, Long lastWakeUpMs, String tagsJson) {
            this.objHash = objHash;
            this.objType = objType;
            this.objFamily = objFamily;
            this.objName = objName;
            this.address = address;
            this.version = version;
            this.alive = alive;
            this.lastWakeUpMs = lastWakeUpMs;
            this.tagsJson = tagsJson;
        }
    }
}