package hi.alert.entity.eai.hipush;

import java.util.ArrayList;

public class HipushRequestVO {
	
    /** 발송채널코드 - SM/KA/PU/RC */
	private String sendChannelType = "";
	/** 통합메시지ID (14자리) */
	private String templateMsgId = "";
	/** 발송메시지제목1 */
	private String msgTitle1 = "";
	/** 발송메시지내용1 */
	private String msgContents1 = "";
	/** 대체메시지제목2 */
	private String msgTitle2 = "";
	/** 대체메시지내용2 */
	private String msgContents2 = "";
	/** 발송요청리스트 */
	private ArrayList<MsgSendReqVo> msgSendReqList;

	public String getMsgTitle1() {
		return msgTitle1;
	}
	public void setMsgTitle1(String msgTitle1) {
		this.msgTitle1 = msgTitle1;
	}

	public String getMsgContents1() {
		return msgContents1;
	}
	public void setMsgContents1(String msgContents1) {
		this.msgContents1 = msgContents1;
	}

	public String getMsgTitle2() {
		return msgTitle2;
	}
	public void setMsgTitle2(String msgTitle2) {
		this.msgTitle2 = msgTitle2;
	}

	public String getMsgContents2() {
		return msgContents2;
	}
	public void setMsgContents2(String msgContents2) {
		this.msgContents2 = msgContents2;
	}

	public String getSendChannelType() {
		return sendChannelType;
	}
	public void setSendChannelType(String sendChannelType) {
		this.sendChannelType = sendChannelType;
	}

	public String getTemplateMsgId() {
		return templateMsgId;
	}
	public void setTemplateMsgId(String templateMsgId) {
		this.templateMsgId = templateMsgId;
	}
	
	public ArrayList<MsgSendReqVo> getMsgSendReqList() {
		return msgSendReqList;
	}
	public void setMsgSendReqList(ArrayList<MsgSendReqVo> msgSendReqList) {
		this.msgSendReqList = msgSendReqList;
	}

}