package com.happy.connectors.gbase;

/**
 * @author DeveloperZJQ
 * @since 2022-6-20
 */
public class MessageEvent {
    private String transId;
    private String cardId;
    private String mobile;
    private String msnTxt;
    private String transType;
    private String code;
    private String name;

    public MessageEvent() {
    }

    public MessageEvent(String transId, String cardId, String mobile, String msnTxt, String transType, String code, String name) {
        this.transId = transId;
        this.cardId = cardId;
        this.mobile = mobile;
        this.msnTxt = msnTxt;
        this.transType = transType;
        this.code = code;
        this.name = name;
    }

    public String getTransId() {
        return transId;
    }

    public void setTransId(String transId) {
        this.transId = transId;
    }

    public String getCardId() {
        return cardId;
    }

    public void setCardId(String cardId) {
        this.cardId = cardId;
    }

    public String getMobile() {
        return mobile;
    }

    public void setMobile(String mobile) {
        this.mobile = mobile;
    }

    public String getMsnTxt() {
        return msnTxt;
    }

    public void setMsnTxt(String msnTxt) {
        this.msnTxt = msnTxt;
    }

    public String getTransType() {
        return transType;
    }

    public void setTransType(String transType) {
        this.transType = transType;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "MessageEvent{" +
                "transId='" + transId + '\'' +
                ", cardId='" + cardId + '\'' +
                ", mobile='" + mobile + '\'' +
                ", msnTxt='" + msnTxt + '\'' +
                ", transType='" + transType + '\'' +
                ", code='" + code + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}
