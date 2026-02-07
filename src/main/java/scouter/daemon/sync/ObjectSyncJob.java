package scouter.daemon.sync;

import com.zaxxer.hikari.HikariDataSource;
import scouter.daemon.ConfigRef;
import scouter.daemon.AppConfig;
import scouter.daemon.sync.ScouterClient.ScouterObject;

import java.sql.*;
import java.util.List;

public class ObjectSyncJob {
    private final ConfigRef cfgRef;
    private final HikariDataSource ds;
    private final ScouterClient client;

    public ObjectSyncJob(ConfigRef cfgRef, HikariDataSource ds, ScouterClient client) {
        this.cfgRef = cfgRef;
        this.ds = ds;
        this.client = client;
    }

    public void runOnceSafe() {
        long start = System.currentTimeMillis();
        try {
            runOnce();
            System.out.println("[sync] ok elapsedMs=" + (System.currentTimeMillis() - start));
        } catch (Throwable t) {
            System.err.println("[sync] fail: " + t.getMessage());
            t.printStackTrace(System.err);
        }
    }

    public void runOnce() throws Exception {
        AppConfig cfg = cfgRef.get();
        List<ScouterObject> objects = client.fetchObjects();
        System.out.println("[sync] fetched count=" + objects.size());

        try (Connection c = ds.getConnection()) {
            c.setAutoCommit(false);
            upsertObjects(c, objects, cfg.batchSize);
            if (cfg.syncMarkInactiveEnabled) {
                markInactiveByLastSeen(c, cfg.inactiveGraceMinutes);
            }
            c.commit();
        }
    }

    private void upsertObjects(Connection c, List<ScouterObject> objects, int batchSize) throws SQLException {
        final String sql =
                "INSERT INTO scouter_object " +
                " (obj_hash, obj_type, obj_family, obj_name, address, version, alive, last_wakeup_ms, last_wakeup_at, tags, first_seen_at, last_seen_at) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS JSON), NOW(3), NOW(3)) " +
                "ON DUPLICATE KEY UPDATE " +
                " obj_type = VALUES(obj_type), " +
                " obj_family = VALUES(obj_family), " +
                " obj_name = VALUES(obj_name), " +
                " address = VALUES(address), " +
                " version = VALUES(version), " +
                " alive = VALUES(alive), " +
                " last_wakeup_ms = VALUES(last_wakeup_ms), " +
                " last_wakeup_at = VALUES(last_wakeup_at), " +
                " tags = VALUES(tags), " +
                " last_seen_at = NOW(3)";

        try (PreparedStatement ps = c.prepareStatement(sql)) {
            int n = 0;
            for (ScouterObject o : objects) {
                ps.setString(1, o.objHash);
                ps.setString(2, o.objType);
                ps.setString(3, o.objFamily);
                ps.setString(4, o.objName);
                ps.setString(5, o.address);
                ps.setString(6, o.version);
                ps.setInt(7, o.alive ? 1 : 0);

                if (o.lastWakeUpMs != null) {
                    ps.setLong(8, o.lastWakeUpMs);
                    ps.setTimestamp(9, new Timestamp(o.lastWakeUpMs));
                } else {
                    ps.setNull(8, Types.BIGINT);
                    ps.setNull(9, Types.TIMESTAMP);
                }

                if (o.tagsJson != null) ps.setString(10, o.tagsJson);
                else ps.setNull(10, Types.VARCHAR);

                ps.addBatch();
                n++;

                if (batchSize > 0 && (n % batchSize == 0)) {
                    ps.executeBatch();
                    ps.clearBatch();
                }
            }
            ps.executeBatch();
            System.out.println("[sync] upserted count=" + n);
        }
    }

    private void markInactiveByLastSeen(Connection c, int graceMinutes) throws SQLException {
        final String sql =
                "UPDATE scouter_object " +
                "SET alive = 0 " +
                "WHERE last_seen_at < (NOW() - INTERVAL ? MINUTE)";

        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setInt(1, graceMinutes);
            int updated = ps.executeUpdate();
            System.out.println("[sync] inactive(updated) = " + updated + " graceMin=" + graceMinutes);
        }
    }
}