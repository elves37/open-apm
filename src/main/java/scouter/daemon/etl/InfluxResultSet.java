package scouter.daemon.etl;

import java.time.Instant;
import java.util.List;
import java.util.Map;

final class InfluxResultSet {
    private final List<EtlRow> rows;

    InfluxResultSet(List<EtlRow> rows) {
        this.rows = rows;
    }

    List<EtlRow> rows() {
        return rows;
    }
}

final class EtlRow {
    final Instant ts;
    final Map<String, String> tags;
    final Map<String, Object> fields;

    EtlRow(Instant ts, Map<String, String> tags, Map<String, Object> fields) {
        this.ts = ts;
        this.tags = tags;
        this.fields = fields;
    }
}