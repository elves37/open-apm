package scouter.daemon.etl;

import scouter.daemon.AppConfig;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

final class InfluxQueryBuilder {

    private final List<String> fields;
    private final List<String> tags;

    InfluxQueryBuilder(AppConfig cfg) {
        this.fields = splitCsv(cfg.get("etl.fields", ""));
        this.tags = splitCsv(cfg.get("etl.tags", "obj,objFamily,objType,objHashTag"));
    }

    String buildQuery(String measurement, Instant from, Instant to) {
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("etl.fields is empty");
        }

        String select = fields.stream()
                .map(f -> "\"" + f + "\"")
                .collect(Collectors.joining(", "));

        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ").append(select).append(" ")
          .append("FROM \"").append(measurement).append("\" ")
          .append("WHERE time >= '").append(from.toString())
          .append("' AND time < '").append(to.toString())
          .append("' ");

        if (!tags.isEmpty()) {
            String groupBy = tags.stream()
                .map(t -> "\"" + t + "\"")
                .collect(Collectors.joining(", "));
            sb.append("GROUP BY ").append(groupBy).append(" ");
        }
        sb.append("ORDER BY time ASC");

        return sb.toString();
    }

    private static List<String> splitCsv(String s) {
        if (s == null) return List.of();
        List<String> out = new ArrayList<>();
        for (String a : s.split(",")) {
            String v = a.trim();
            if (!v.isEmpty()) out.add(v);
        }
        return out;
    }
}
