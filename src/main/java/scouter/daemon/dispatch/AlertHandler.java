package scouter.daemon.dispatch;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import scouter.daemon.AppConfig;
import scouter.daemon.ConfigRef;
import scouter.daemon.dispatch.SubscriptionDao.DispatchContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlertHandler {
    private static final Logger log = LoggerFactory.getLogger(AlertHandler.class);

    private final ConfigRef cfgRef;
    private final ObjectMapper om;
    private final SubscriptionDao dao;

    private final AlertPreprocessor preprocessor;
    private final HipushSmsSender smsSender;

    public AlertHandler(ConfigRef cfgRef, ObjectMapper om, SubscriptionDao dao) {
        this.cfgRef = cfgRef;
        this.om = om;
        this.dao = dao;
        this.preprocessor = new AlertPreprocessor();
        this.smsSender = new HipushSmsSender();
    }

    public boolean handle(ConsumerRecord<String, String> r) {
        try {
            AlertEvent ev = AlertEvent.parse(om, r.value());
            if (ev.objHash == null || ev.objHash.trim().isEmpty()) {
                log.error("[dispatch] skip: missing objHash payload=" + AlertPreprocessor.truncate(r.value(), 300));
                return true;
            }

            DispatchContext ctx = dao.fetchRecipientsAndObject(ev.objHash);
            if (ctx.phones == null || ctx.phones.isEmpty()) return true;

            AppConfig cfg = cfgRef.get();
            PreparedAlert pa = preprocessor.preprocess(cfg, ev, ctx);
            if (pa == null) {
                log.info("[dropped] objHash=" + ev.objHash + " title=" + ev.title);
                return true;
            }

            if (cfg.smsEnabled) {
                return smsSender.send(cfg, ctx, pa);
            } else {
                log.info("[dispatch] title=" + pa.title + " message=" + pa.message + " name=" + pa.toName
                        + " time=" + pa.occurredAt + " level=" + pa.severity);
                return true;
            }
        } catch (Exception e) {
            log.error("[dispatch] failed offset=" + r.offset() + " err=" + e);
            return false;
        }
    }
}