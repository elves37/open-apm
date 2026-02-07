package scouter.daemon.codec;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.nio.charset.StandardCharsets;

public final class HipushJsonMapper {

    @SuppressWarnings("deprecation")
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            // JSON에 VO에 없는 필드가 있어도 에러 안 내고 무시 (API 변경에 강함)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private HipushJsonMapper() {
    }

    // VO -> JSON
    public static String toJson(Object vo) {
        try {
            return MAPPER.writeValueAsString(vo);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to serialize VO to JSON", e);
        }
    }

    public static byte[] toBytes(Object vo) {
        return toJson(vo).getBytes(StandardCharsets.UTF_8);
    }

    // JSON -> VO (가장 기본)
    public static <T> T fromJson(String json, Class<T> type) {
        try {
            return MAPPER.readValue(json, type);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to deserialize JSON to " + type.getSimpleName(), e);
        }
    }

    // JSON -> VO (제네릭 타입: List<MsgSendReqVo> 같은 케이스)
    public static <T> T fromJson(String json, TypeReference<T> typeRef) {
        try {
            return MAPPER.readValue(json, typeRef);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to deserialize JSON to generic type", e);
        }
    }

    // bytes -> VO
    public static <T> T fromBytes(byte[] bytes, Class<T> type) {
        return fromJson(new String(bytes, StandardCharsets.UTF_8), type);
    }

    public static <T> T fromBytes(byte[] bytes, TypeReference<T> typeRef) {
        return fromJson(new String(bytes, StandardCharsets.UTF_8), typeRef);
    }

    public static ObjectMapper objectMapper() {
        return MAPPER;
    }
}