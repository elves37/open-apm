package scouter.daemon.etl;

import scouter.daemon.AppConfig;

import java.util.HashMap;
import java.util.Map;

final class AggSpec {
    private final Map<String, String> fieldAgg;

    AggSpec(Map<String, String> fieldAgg) {
        this.fieldAgg = fieldAgg;
    }

    String aggOf(String field, String defaultAgg) {
        String v = fieldAgg.get(field);
        return (v == null || v.isBlank()) ? defaultAgg : v.trim();
    }

    static AggSpec from(AppConfig cfg) {
        String raw = cfg.get("etl.fieldAgg", "");
        Map<String, String> m = new HashMap<>();

        for (String part : raw.split(",")) {
            String s = part.trim();
            if (s.isEmpty()) continue;
            int ix = s.indexOf('=');
            if (ix <= 0) continue;

            String k = s.substring(0, ix).trim();
            String v = s.substring(ix + 1).trim();
            if (!k.isEmpty() && !v.isEmpty()) m.put(k, v);
        }

        return new AggSpec(m);
    }
}