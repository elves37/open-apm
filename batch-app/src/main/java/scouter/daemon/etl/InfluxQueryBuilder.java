package scouter.daemon.etl;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

final class InfluxQueryBuilder {

    String buildQuery(
            String measurement,
            List<String> fields,
            List<String> tags,
            Instant from,
            Instant to
    ) {
        if (fields == null || fields.isEmpty()) {
            throw new IllegalArgumentException("etl.fields is empty");
        }

        String select = fields.stream()
                .map(f -> "\"" + f + "\"")
                .collect(Collectors.joining(", "));

        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ").append(select).append(" ")
          .append("FROM \"").append(measurement).append("\" ")
          .append("WHERE time >= '").append(from.toString()).append("' ")
          .append("AND time < '").append(to.toString()).append("' ");

        if (tags != null && !tags.isEmpty()) {
            String groupBy = tags.stream()
                    .map(t -> "\"" + t + "\"")
                    .collect(Collectors.joining(", "));
            sb.append("GROUP BY ").append(groupBy).append(" ");
        }

        sb.append("ORDER BY time ASC");
        return sb.toString();
    }

    static List<String> splitCsv(String s) {
        List<String> out = new ArrayList<>();
        if (s == null || s.isBlank()) {
            return out;
        }
        for (String a : s.split(",")) {
            String v = a.trim();
            if (!v.isEmpty()) {
                out.add(v);
            }
        }
        return out;
    }
}