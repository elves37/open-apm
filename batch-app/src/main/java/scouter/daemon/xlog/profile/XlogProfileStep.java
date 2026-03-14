package scouter.daemon.xlog.profile;

public class XlogProfileStep {

    private String stepClass;
    private Integer stepType;
    private String stepTypeName;

    private Integer start_time;
    private Integer elapsed;
    private Long cputime;

    private Integer hash;
    private String hashText;

    private Integer method;
    private String methodName;

    private Integer sql;
    private String sqlText;

    private Integer apicall;
    private String apiCallName;

    private Integer error;
    private String errorText;

    private String message;
    private String ipaddr;
    private Integer port;
    private String address;
    private String txid;

    public String getStepClass() {
        return stepClass;
    }

    public void setStepClass(String stepClass) {
        this.stepClass = stepClass;
    }

    public Integer getStepType() {
        return stepType;
    }

    public void setStepType(Integer stepType) {
        this.stepType = stepType;
    }

    public String getStepTypeName() {
        return stepTypeName;
    }

    public void setStepTypeName(String stepTypeName) {
        this.stepTypeName = stepTypeName;
    }

    public Integer getStart_time() {
        return start_time;
    }

    public void setStart_time(Integer start_time) {
        this.start_time = start_time;
    }

    public Integer getElapsed() {
        return elapsed;
    }

    public void setElapsed(Integer elapsed) {
        this.elapsed = elapsed;
    }

    public Long getCputime() {
        return cputime;
    }

    public void setCputime(Long cputime) {
        this.cputime = cputime;
    }

    public Integer getHash() {
        return hash;
    }

    public void setHash(Integer hash) {
        this.hash = hash;
    }

    public String getHashText() {
        return hashText;
    }

    public void setHashText(String hashText) {
        this.hashText = hashText;
    }

    public Integer getMethod() {
        return method;
    }

    public void setMethod(Integer method) {
        this.method = method;
    }

    public String getMethodName() {
        return methodName;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    public Integer getSql() {
        return sql;
    }

    public void setSql(Integer sql) {
        this.sql = sql;
    }

    public String getSqlText() {
        return sqlText;
    }

    public void setSqlText(String sqlText) {
        this.sqlText = sqlText;
    }

    public Integer getApicall() {
        return apicall;
    }

    public void setApicall(Integer apicall) {
        this.apicall = apicall;
    }

    public String getApiCallName() {
        return apiCallName;
    }

    public void setApiCallName(String apiCallName) {
        this.apiCallName = apiCallName;
    }

    public Integer getError() {
        return error;
    }

    public void setError(Integer error) {
        this.error = error;
    }

    public String getErrorText() {
        return errorText;
    }

    public void setErrorText(String errorText) {
        this.errorText = errorText;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getIpaddr() {
        return ipaddr;
    }

    public void setIpaddr(String ipaddr) {
        this.ipaddr = ipaddr;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getTxid() {
        return txid;
    }

    public void setTxid(String txid) {
        this.txid = txid;
    }
}