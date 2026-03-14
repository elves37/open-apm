package scouter.daemon.xlog.profile;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XlogProfileDao {

    private static final Logger log = LoggerFactory.getLogger(XlogProfileDao.class);

    private final DataSource dataSource;
    private final ObjectMapper objectMapper;
    private final String headerTable;
    private final String stepTable;
    private final String rejectTable;
    private final boolean storeRawBase64;
    private final boolean storePayloadJson;
    private final boolean storeStepRawJson;
    private final int headerBatchSize;
    private final int stepBatchSize;

    public XlogProfileDao(
            DataSource dataSource,
            ObjectMapper objectMapper,
            String headerTable,
            String stepTable,
            String rejectTable,
            boolean storeRawBase64,
            boolean storePayloadJson,
            boolean storeStepRawJson,
            int headerBatchSize,
            int stepBatchSize
    ) {
        this.dataSource = dataSource;
        this.objectMapper = objectMapper;
        this.headerTable = headerTable;
        this.stepTable = stepTable;
        this.rejectTable = rejectTable;
        this.storeRawBase64 = storeRawBase64;
        this.storePayloadJson = storePayloadJson;
        this.storeStepRawJson = storeStepRawJson;
        this.headerBatchSize = Math.max(1, headerBatchSize);
        this.stepBatchSize = Math.max(1, stepBatchSize);
    }

    public void saveBatch(List<XlogProfileMessage> messages) throws Exception {
        if (messages == null || messages.isEmpty()) {
            return;
        }

        List<XlogProfileMessage> ordered = new ArrayList<>(messages);
        ordered.sort(Comparator
                .comparingLong((XlogProfileMessage m) -> m.getTime() == null ? 0L : m.getTime())
                .thenComparing(m -> nullSafe(m.getTxid()))
                .thenComparingInt(m -> nvlInt(m.getObjHash())));

        Connection conn = null;
        PreparedStatement headerPs = null;
        PreparedStatement stepPs = null;

        try {
            conn = dataSource.getConnection();
            conn.setAutoCommit(false);

            headerPs = conn.prepareStatement(headerInsertSql());
            stepPs = conn.prepareStatement(stepInsertSql());

            int headerBuffered = 0;
            int stepBuffered = 0;

            for (XlogProfileMessage message : ordered) {
                bindHeader(headerPs, message);
                headerPs.addBatch();
                headerBuffered++;

                if (headerBuffered >= headerBatchSize) {
                    headerPs.executeBatch();
                    headerBuffered = 0;
                }

                List<XlogProfileStep> steps = message.getSteps();
                if (steps == null || steps.isEmpty()) {
                    continue;
                }

                for (int i = 0; i < steps.size(); i++) {
                    bindStep(stepPs, message, steps.get(i), i);
                    stepPs.addBatch();
                    stepBuffered++;

                    if (stepBuffered >= stepBatchSize) {
                        stepPs.executeBatch();
                        stepBuffered = 0;
                    }
                }
            }

            if (headerBuffered > 0) {
                headerPs.executeBatch();
            }
            if (stepBuffered > 0) {
                stepPs.executeBatch();
            }

            conn.commit();
        } catch (Exception e) {
            rollbackQuietly(conn);
            throw e;
        } finally {
            closeQuietly(stepPs);
            closeQuietly(headerPs);
            closeQuietly(conn);
        }
    }

    public void saveReject(String topic, int partition, long offset, String payloadJson, String errorMessage) {
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            conn = dataSource.getConnection();
            ps = conn.prepareStatement(
                    "INSERT IGNORE INTO " + rejectTable + " (topic_name, partition_no, offset_no, error_message, payload_json) " +
                            "VALUES (?, ?, ?, ?, ?) "
                            // + "ON DUPLICATE KEY UPDATE error_message = VALUES(error_message), payload_json = VALUES(payload_json)"
            );
            ps.setString(1, topic);
            ps.setInt(2, partition);
            ps.setLong(3, offset);
            ps.setString(4, cut(errorMessage, 4000));
            ps.setString(5, payloadJson);
            ps.executeUpdate();
        } catch (Exception e) {
            log.error("failed to save reject. topic={}, partition={}, offset={}", topic, partition, offset, e);
        } finally {
            closeQuietly(ps);
            closeQuietly(conn);
        }
    }

    private String headerInsertSql() {
        return "INSERT IGNORE INTO " + headerTable + " (" +
                "event_time, txid, obj_hash, yyyymmdd, obj_name, service_hash, service_name, txid_hex, " +
                "elapsed_ms, profile_raw_length, profile_raw_base64, payload_json" +
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    }

    private String stepInsertSql() {
        return "INSERT IGNORE INTO " + stepTable + " (" +
                "event_time, txid, obj_hash, step_index, service_hash, " +
                "step_class, step_type, step_type_name, start_time_ms, elapsed_ms, cputime, " +
                "hash_value, hash_text, method_hash, method_name, sql_hash, sql_text, " +
                "api_call_hash, api_call_name, error_hash, error_text, message_value, ipaddr, port, " +
                "address_value, txid_value, raw_json" +
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    }

    private void bindHeader(PreparedStatement ps, XlogProfileMessage message) throws Exception {
        ps.setTimestamp(1, new Timestamp(message.getTime().longValue()));
        ps.setString(2, message.getTxid());
        ps.setInt(3, nvlInt(message.getObjHash()));
        ps.setString(4, message.getDate());
        ps.setString(5, message.getObjName());
        ps.setInt(6, nvlInt(message.getServiceHash()));
        ps.setString(7, message.getServiceName());
        ps.setString(8, message.getTxidHex());
        setNullableInt(ps, 9, message.getElapsed());
        setNullableInt(ps, 10, message.getProfileRawLength());

        if (storeRawBase64) {
            ps.setString(11, message.getProfileRawBase64());
        } else {
            ps.setNull(11, Types.LONGVARCHAR);
        }

        if (storePayloadJson) {
            ps.setString(12, objectMapper.writeValueAsString(message));
        } else {
            ps.setNull(12, Types.LONGVARCHAR);
        }
    }

    private void bindStep(PreparedStatement ps, XlogProfileMessage message, XlogProfileStep step, int stepIndex) throws Exception {
        ps.setTimestamp(1, new Timestamp(message.getTime().longValue()));
        ps.setString(2, message.getTxid());
        ps.setInt(3, nvlInt(message.getObjHash()));
        ps.setInt(4, stepIndex);
        ps.setInt(5, nvlInt(message.getServiceHash()));
        ps.setString(6, step.getStepClass());
        setNullableInt(ps, 7, step.getStepType());
        ps.setString(8, step.getStepTypeName());
        setNullableInt(ps, 9, step.getStart_time());
        setNullableInt(ps, 10, step.getElapsed());
        setNullableLong(ps, 11, step.getCputime());
        setNullableInt(ps, 12, step.getHash());
        ps.setString(13, step.getHashText());
        setNullableInt(ps, 14, step.getMethod());
        ps.setString(15, step.getMethodName());
        setNullableInt(ps, 16, step.getSql());
        ps.setString(17, step.getSqlText());
        setNullableInt(ps, 18, step.getApicall());
        ps.setString(19, step.getApiCallName());
        setNullableInt(ps, 20, step.getError());
        ps.setString(21, step.getErrorText());
        ps.setString(22, step.getMessage());
        ps.setString(23, step.getIpaddr());
        setNullableInt(ps, 24, step.getPort());
        ps.setString(25, step.getAddress());
        ps.setString(26, step.getTxid());

        if (storeStepRawJson) {
            ps.setString(27, objectMapper.writeValueAsString(step));
        } else {
            ps.setNull(27, Types.LONGVARCHAR);
        }
    }

    private static int nvlInt(Integer value) {
        return value == null ? 0 : value;
    }

    private static String nullSafe(String value) {
        return value == null ? "" : value;
    }

    private static String cut(String value, int max) {
        if (value == null) {
            return null;
        }
        if (value.length() <= max) {
            return value;
        }
        return value.substring(0, max);
    }

    private static void setNullableInt(PreparedStatement ps, int index, Integer value) throws Exception {
        if (value == null) {
            ps.setNull(index, Types.INTEGER);
        } else {
            ps.setInt(index, value);
        }
    }

    private static void setNullableLong(PreparedStatement ps, int index, Long value) throws Exception {
        if (value == null) {
            ps.setNull(index, Types.BIGINT);
        } else {
            ps.setLong(index, value);
        }
    }

    private static void rollbackQuietly(Connection conn) {
        if (conn == null) {
            return;
        }
        try {
            conn.rollback();
        } catch (Exception ignore) {
        }
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception ignore) {
        }
    }
}
