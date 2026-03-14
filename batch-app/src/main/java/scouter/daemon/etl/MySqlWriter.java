package scouter.daemon.etl;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

final class MySqlWriter {

    private final DataSource ds;

    MySqlWriter(DataSource ds) {
        this.ds = ds;
    }

    int upsertAll(String table, List<String> tagKeys, List<String> fieldKeys, List<EtlRow> rows) throws Exception {
        if (rows == null || rows.isEmpty()) {
            return 0;
        }
        if (fieldKeys == null || fieldKeys.isEmpty()) {
            throw new IllegalArgumentException("etl.fields is empty");
        }

        List<String> distinctTags = splitCsvDistinct(tagKeys);
        List<String> distinctFields = splitCsvDistinct(fieldKeys);

        Columns cols = Columns.of(table, distinctTags, distinctFields);
        String sql = buildUpsertSql(table, cols);

        int cnt = 0;
        try (Connection c = ds.getConnection()) {
            c.setAutoCommit(false);
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                for (EtlRow r : rows) {
                    bind(ps, r, cols);
                    ps.addBatch();
                    cnt++;

                    if (cnt % 1000 == 0) {
                        ps.executeBatch();
                        c.commit();
                    }
                }
                ps.executeBatch();
                c.commit();
            } catch (Exception e) {
                try {
                    c.rollback();
                } catch (Exception ignore) {
                }
                throw e;
            }
        }

        return cnt;
    }

    private void bind(PreparedStatement ps, EtlRow r, Columns cols) throws Exception {
        int i = 1;

        ps.setTimestamp(i++, Timestamp.from(r.ts));

        for (String tk : cols.tagKeys) {
            Object v = r.tags.get(tk);
            if (v == null) {
                ps.setNull(i++, Types.VARCHAR);
            } else {
                ps.setString(i++, v.toString());
            }
        }

        for (String fk : cols.fieldCols) {
            Object v = r.fields.get(fk);
            setField(ps, i++, v);
        }

        int expected = getParamCount(ps);
        if (expected > 0) {
            int bound = i - 1;
            if (bound != expected) {
                throw new SQLException(
                        "Bind parameter count mismatch. bound=" + bound +
                        ", expected=" + expected +
                        ", tags=" + cols.tagKeys.size() +
                        ", fields=" + cols.fieldCols.size() +
                        ", table=" + cols.tableDebug
                );
            }
        }
    }

    private static void setField(PreparedStatement ps, int idx, Object v) throws SQLException {
        if (v == null) {
            ps.setNull(idx, Types.DOUBLE);
            return;
        }
        ps.setObject(idx, normalize(v));
    }

    private static Object normalize(Object v) {
        if (v == null) {
            return null;
        }
        if (v instanceof Number) {
            return v;
        }
        if (v instanceof Boolean) {
            return ((Boolean) v) ? 1 : 0;
        }
        return v.toString();
    }

    private static int getParamCount(PreparedStatement ps) {
        try {
            ParameterMetaData pmd = ps.getParameterMetaData();
            return (pmd == null) ? -1 : pmd.getParameterCount();
        } catch (Exception e) {
            return -1;
        }
    }

    private String buildUpsertSql(String table, Columns cols) {
        String colList = joinCols(cols.allCols);
        String placeholders = placeholders(cols.allCols.size());

        StringBuilder update = new StringBuilder();
        for (int i = 0; i < cols.fieldCols.size(); i++) {
            String fk = cols.fieldCols.get(i);
            if (i > 0) {
                update.append(", ");
            }
            update.append(quote(fk)).append("=VALUES(").append(quote(fk)).append(")");
        }

        return "INSERT INTO " + table + " (" + colList + ") VALUES (" + placeholders + ") " +
                "ON DUPLICATE KEY UPDATE " + update;
    }

    private String joinCols(List<String> cols) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < cols.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(quote(cols.get(i)));
        }
        return sb.toString();
    }

    private String placeholders(int n) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append("?");
        }
        return sb.toString();
    }

    private static String quote(String col) {
        return "`" + col + "`";
    }

    private static List<String> splitCsvDistinct(List<String> values) {
        LinkedHashSet<String> out = new LinkedHashSet<>();
        if (values == null) {
            return List.of();
        }
        for (String v : values) {
            if (v == null) {
                continue;
            }
            String s = v.trim();
            if (!s.isEmpty()) {
                out.add(s);
            }
        }
        return new ArrayList<>(out);
    }

    static final class Columns {
        final String tableDebug;
        final List<String> tagKeys;
        final List<String> fieldCols;
        final List<String> allCols;

        private Columns(
                String tableDebug,
                List<String> tagKeys,
                List<String> fieldCols,
                List<String> allCols
        ) {
            this.tableDebug = tableDebug;
            this.tagKeys = tagKeys;
            this.fieldCols = fieldCols;
            this.allCols = allCols;
        }

        static Columns of(String table, List<String> tagKeys, List<String> fieldKeys) {
            List<String> all = new ArrayList<>();
            all.add("ts");
            all.addAll(tagKeys);
            all.addAll(fieldKeys);
            return new Columns(table, tagKeys, fieldKeys, all);
        }
    }
}