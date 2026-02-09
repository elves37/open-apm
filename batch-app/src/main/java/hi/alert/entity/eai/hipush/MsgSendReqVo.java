package hi.alert.entity.eai.hipush;

public class MsgSendReqVo {
	
	/** TMS연동키 */
	private String tmsIfKey = "";
	/** 수신고객ID */
	private String recvCustId = "";
	/** 수신고객핸드폰번호 */
	private String rcvSmsNo = "";
	/** 수신고객명 */
	private String rcverNm = "";
	/** 발신자사번 */
	private String senderId = "";
	/** 발신/회신 번호 */
	private String sendSmsNo = "";
	/** 개별예약시간 */
	private String reserveDate = "";
	
	public String getTmsIfKey() {
		return tmsIfKey;
	}
	public void setTmsIfKey(String tmsIfKey) {
		this.tmsIfKey = tmsIfKey;
	}
	public String getRecvCustId() {
		return recvCustId;
	}
	public void setRecvCustId(String recvCustId) {
		this.recvCustId = recvCustId;
	}
	public String getRcverNm() {
		return rcverNm;
	}
	public void setRcverNm(String rcverNm) {
		this.rcverNm = rcverNm;
	}
	public String getRcvSmsNo() {
		return rcvSmsNo;
	}
	public void setRcvSmsNo(String rcvSmsNo) {
		this.rcvSmsNo = rcvSmsNo;
	}
	public String getSenderId() {
		return senderId;
	}
	public void setSenderId(String senderId) {
		this.senderId = senderId;
	}
	public String getSendSmsNo() {
		return sendSmsNo;
	}
	public void setSendSmsNo(String sendSmsNo) {
		this.sendSmsNo = sendSmsNo;
	}
	public String getReserveDate() {
		return reserveDate;
	}
	public void setReserveDate(String reserveDate) {
		this.reserveDate = reserveDate;
	}

}
