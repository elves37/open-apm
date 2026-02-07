package scouter.daemon.etl;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import scouter.daemon.AppConfig;
import scouter.daemon.ConfigRef;

final class InfluxReader implements AutoCloseable {

    private final ConfigRef cfgRef;
    private final EtlLog log;

    private volatile InfluxDB influx;
    private volatile String database;
    private volatile String sig;

    InfluxReader(ConfigRef cfgRef, EtlLog log) {
        this.cfgRef = cfgRef;
        this.log = log;
        reconnect(cfgRef.get());
    }

    InfluxResultSet query(String influxQl) {
        AppConfig cfg = cfgRef.get();
        String s = signature(cfg);
        if (!s.equals(sig)) {
            reconnect(cfg);
        }

        Query q = new Query(influxQl, database);
        QueryResult r = influx.query(q);
        return new InfluxResultSet(InfluxResultMapper.map(r));
    }

    private void reconnect(AppConfig cfg) {
        closeQuiet();

        String url = cfg.get("etl.influx.url", "http://127.0.0.1:8086").trim();
        String user = cfg.get("etl.influx.user", "").trim();
        String pass = cfg.get("etl.influx.pass", "").trim();
        this.database = cfg.get("etl.influx.db", "scouter").trim();
        this.sig = signature(cfg);

        if (user.isEmpty() && pass.isEmpty()) {
            this.influx = InfluxDBFactory.connect(url);
        } else {
            this.influx = InfluxDBFactory.connect(url, user, pass);
        }

        log.info("[etl] influx connected db=" + database);
    }

    private String signature(AppConfig cfg) {
        String url = cfg.get("etl.influx.url", "http://127.0.0.1:8086").trim();
        String user = cfg.get("etl.influx.user", "").trim();
        String pass = cfg.get("etl.influx.pass", "").trim();
        String db = cfg.get("etl.influx.db", "scouter").trim();
        return url + "|" + user + "|" + pass + "|" + db;
    }

    private void closeQuiet() {
        try {
            if (influx != null) influx.close();
        } catch (Exception ignore) {}
    }

    @Override
    public void close() {
        closeQuiet();
    }
}