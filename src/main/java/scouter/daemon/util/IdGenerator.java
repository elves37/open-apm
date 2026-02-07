package scouter.daemon.util;

import java.security.SecureRandom;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public final class IdGenerator {

    private static final ZoneId KST = ZoneId.of("Asia/Seoul");
    private static final DateTimeFormatter TS_FMT = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    private static final SecureRandom RNG = new SecureRandom();

    private IdGenerator() {}

    // 14자리 문자열 + 현재일시(yyyyMMddHHmmss) + 10자리 난수
    public static String build(String input14) {
        if (input14 == null || input14.length() != 14) {
            throw new IllegalArgumentException("input must be a 14-character string");
        }

        String ts = ZonedDateTime.now(KST).format(TS_FMT);
        String rand10 = randomDigits(10);

        return input14 + ts + rand10;
    }

    private static String randomDigits(int len) {
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            sb.append(RNG.nextInt(10));
        }
        return sb.toString();
    }
}