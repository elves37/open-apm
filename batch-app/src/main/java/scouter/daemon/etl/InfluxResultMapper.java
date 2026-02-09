package scouter.daemon.etl;

import org.influxdb.dto.QueryResult;

import java.time.Instant;
import java.util.*;

final class InfluxResultMapper {

    static List<EtlRow> map(QueryResult r) {
        List<EtlRow> out = new ArrayList<>();
        if (r == null || r.getResults() == null) return out;

        for (QueryResult.Result res : r.getResults()) {
            if (res == null || res.getSeries() == null) continue;

            for (QueryResult.Series s : res.getSeries()) {
                List<String> cols = s.getColumns();
                List<List<Object>> values = s.getValues();
                Map<String, String> tags = (s.getTags() == null) ? Map.of() : s.getTags();

                if (cols == null || values == null) continue;

                int timeIdx = cols.indexOf("time");

                for (List<Object> row : values) {
                    if (row == null) continue;

                    Instant ts = null;
                    if (timeIdx >= 0 && timeIdx < row.size()) {
                        Object tv = row.get(timeIdx);
                        if (tv != null) ts = Instant.parse(tv.toString());
                    }
                    if (ts == null) continue;

                    Map<String, Object> fields = new HashMap<>();
                    for (int i = 0; i < cols.size() && i < row.size(); i++) {
                        String c = cols.get(i);
                        if ("time".equals(c)) continue;
                        fields.put(c, row.get(i));
                    }

                    out.add(new EtlRow(ts, tags, fields));
                }
            }
        }

        return out;
    }
}