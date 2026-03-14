package scouter.daemon.xlog.profile;

import java.util.ArrayList;
import java.util.List;

public class XlogProfileMessage {

    private String plugin;
    private Long time;
    private String timeText;
    private String date;

    private Integer objHash;
    private String objHashHex;
    private String objName;

    private Integer serviceHash;
    private String serviceName;

    private String txid;
    private String txidHex;
    private Integer elapsed;

    private String profileRawBase64;
    private Integer profileRawLength;

    private List<XlogProfileStep> steps = new ArrayList<XlogProfileStep>();

    public String getPlugin() {
        return plugin;
    }

    public void setPlugin(String plugin) {
        this.plugin = plugin;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public String getTimeText() {
        return timeText;
    }

    public void setTimeText(String timeText) {
        this.timeText = timeText;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public Integer getObjHash() {
        return objHash;
    }

    public void setObjHash(Integer objHash) {
        this.objHash = objHash;
    }

    public String getObjHashHex() {
        return objHashHex;
    }

    public void setObjHashHex(String objHashHex) {
        this.objHashHex = objHashHex;
    }

    public String getObjName() {
        return objName;
    }

    public void setObjName(String objName) {
        this.objName = objName;
    }

    public Integer getServiceHash() {
        return serviceHash;
    }

    public void setServiceHash(Integer serviceHash) {
        this.serviceHash = serviceHash;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getTxid() {
        return txid;
    }

    public void setTxid(String txid) {
        this.txid = txid;
    }

    public String getTxidHex() {
        return txidHex;
    }

    public void setTxidHex(String txidHex) {
        this.txidHex = txidHex;
    }

    public Integer getElapsed() {
        return elapsed;
    }

    public void setElapsed(Integer elapsed) {
        this.elapsed = elapsed;
    }

    public String getProfileRawBase64() {
        return profileRawBase64;
    }

    public void setProfileRawBase64(String profileRawBase64) {
        this.profileRawBase64 = profileRawBase64;
    }

    public Integer getProfileRawLength() {
        return profileRawLength;
    }

    public void setProfileRawLength(Integer profileRawLength) {
        this.profileRawLength = profileRawLength;
    }

    public List<XlogProfileStep> getSteps() {
        return steps;
    }

    public void setSteps(List<XlogProfileStep> steps) {
        this.steps = steps;
    }
}