package scouter.daemon.dispatch;

import hi.alert.entity.eai.hipush.HipushRequestVO;
import hi.alert.entity.eai.hipush.MsgSendReqVo;
import hi.internal.process.outbound.OutboundProcess;
import scouter.daemon.AppConfig;
import scouter.daemon.codec.HipushJsonMapper;
import scouter.daemon.dispatch.SubscriptionDao.DispatchContext;
import scouter.daemon.util.IdGenerator;

import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HipushSmsSender {
    private static final Logger log = LoggerFactory.getLogger(HipushSmsSender.class);

    public boolean send(AppConfig cfg, DispatchContext ctx, PreparedAlert pa) {
        try {
            Properties p = cfg.raw();

            String ifId = get(p, "hipush.ifId", "IR_HI-APM_0004");
            String sendChannelType = get(p, "hipush.send.channelType", "PU");
            String templateMsgId = get(p, "hipush.template.msgId", "ZZPU1260114440");
            String serviceUserId = get(p, "hipush.service.userId", "999999");
            String receiverName = get(p, "hipush.receiver.name", "OPENAPM");
            String senderPhone = get(p, "hipush.sender.phone", "0226284567");

            HipushRequestVO reqVo = new HipushRequestVO();
            reqVo.setSendChannelType(sendChannelType);
            reqVo.setTemplateMsgId(templateMsgId);
            reqVo.setMsgTitle1(pa.title);
            reqVo.setMsgContents1(pa.text);

            ArrayList<MsgSendReqVo> msgSendReqList = new ArrayList<>();
            for (String hp : ctx.phones) {
                MsgSendReqVo m = new MsgSendReqVo();
                m.setTmsIfKey(IdGenerator.build(templateMsgId));
                m.setRecvCustId(hp);
                m.setRcverNm(receiverName);
                m.setRcvSmsNo(hp);
                m.setSenderId(ctx.uid);
                m.setSendSmsNo(senderPhone);
                msgSendReqList.add(m);
            }
            reqVo.setMsgSendReqList(msgSendReqList);

            String reqJson = HipushJsonMapper.toJson(reqVo);

            OutboundProcess service = new OutboundProcess();
            service.setUserId(serviceUserId);

            HttpResponse<String> res = service.call(cfg.cisInternalURL, ifId, reqJson);
            int status = res.statusCode();
            String body = res.body();

            if (status >= 200 && status < 300) {
                log.info("[hipush] ok status=" + status + " body=" + body);
                return true;
            } else {
                log.info("[hipush] error status=" + status + " body=" + body);
                return false;
            }
        } catch (Exception e) {
            log.error("[hipush] send failed: " + e);
            return false;
        }
    }

    private static String get(Properties p, String key, String def) {
        String v = p.getProperty(key);
        if (v == null || v.trim().isEmpty()) return def;
        return v.trim();
    }
}