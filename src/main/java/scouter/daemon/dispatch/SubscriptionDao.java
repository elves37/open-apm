package scouter.daemon.dispatch;

import com.zaxxer.hikari.HikariDataSource;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class SubscriptionDao {
    private final HikariDataSource ds;

    public SubscriptionDao(HikariDataSource ds) {
        this.ds = ds;
    }

    public DispatchContext fetchRecipientsAndObject(String objHash) throws SQLException {
        final String sql =
                "SELECT u.hp AS hp, u.id AS u_id, o.obj_name AS obj_name, o.obj_type AS obj_type, o.address AS address " +
                "  FROM apm_object_subscription s " +
                "  JOIN apm_user u ON u.id = s.user_id " +
                "  LEFT JOIN scouter_object o ON o.obj_hash = s.obj_hash " +
                " WHERE s.obj_hash = ? " +
                "   AND s.enabled = 1 " +
                "   AND u.hp IS NOT NULL AND u.hp <> ''";

        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, objHash);

            try (ResultSet rs = ps.executeQuery()) {
                List<String> phones = new ArrayList<>();
                String uid = null;
                String objName = null;
                String objType = null;
                String address = null;

                while (rs.next()) {
                    String hp = rs.getString("hp");
                    if (hp != null && !hp.trim().isEmpty()) phones.add(hp.trim().replace("-", ""));

                    if (uid == null) uid = rs.getString("u_id");
                    if (objName == null) objName = rs.getString("obj_name");
                    if (objType == null) objType = rs.getString("obj_type");
                    if (address == null) address = rs.getString("address");
                }

                return new DispatchContext(phones, uid, objName, objType, address);
            }
        }
    }

    public static final class DispatchContext {
        public final List<String> phones;
        public final String uid;
        public final String objName;
        public final String objType;
        public final String address;

        public DispatchContext(List<String> phones, String uid, String objName, String objType, String address) {
            this.phones = phones;
            this.uid = uid;
            this.objName = objName;
            this.objType = objType;
            this.address = address;
        }
    }
}