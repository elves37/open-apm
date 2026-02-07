package scouter.daemon.etl;

final class EtlLog {

    private final Object slf4jLogger;
    private final boolean slf4jAvailable;

    private EtlLog(Object slf4jLogger, boolean slf4jAvailable) {
        this.slf4jLogger = slf4jLogger;
        this.slf4jAvailable = slf4jAvailable;
    }

    static EtlLog of(Class<?> cls) {
        try {
            Class<?> factory = Class.forName("org.slf4j.LoggerFactory");
            Object logger = factory.getMethod("getLogger", Class.class).invoke(null, cls);
            return new EtlLog(logger, true);
        } catch (Throwable t) {
            return new EtlLog(null, false);
        }
    }

    void info(String msg) {
        if (slf4jAvailable) {
            try {
                slf4jLogger.getClass().getMethod("info", String.class).invoke(slf4jLogger, msg);
                return;
            } catch (Throwable ignore) {}
        }
        System.out.println(msg);
    }

    void warn(String msg) {
        if (slf4jAvailable) {
            try {
                slf4jLogger.getClass().getMethod("warn", String.class).invoke(slf4jLogger, msg);
                return;
            } catch (Throwable ignore) {}
        }
        System.out.println(msg);
    }

    void error(String msg, Throwable t) {
        if (slf4jAvailable) {
            try {
                slf4jLogger.getClass().getMethod("error", String.class, Throwable.class).invoke(slf4jLogger, msg, t);
                return;
            } catch (Throwable ignore) {}
        }
        System.out.println(msg);
        if (t != null) t.printStackTrace(System.out);
    }
}