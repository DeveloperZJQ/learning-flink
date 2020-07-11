package com.stream.model;

import java.io.Serializable;

/**
 * @author happy
 * @create 2020-07-08 06:21
 */
public  class TransInfo implements Serializable {
    private long timeStamp;
    private String userId;
    private String cardId;
    private String transfer;
    private String fee;

    public TransInfo() {
    }

    public TransInfo(long timeStamp, String userId, String cardId, String transfer, String fee) {
        this.timeStamp = timeStamp;
        this.userId = userId;
        this.cardId = cardId;
        this.transfer = transfer;
        this.fee = fee;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getCardId() {
        return cardId;
    }

    public void setCardId(String cardId) {
        this.cardId = cardId;
    }

    public String getTransfer() {
        return transfer;
    }

    public void setTransfer(String transfer) {
        this.transfer = transfer;
    }

    public String getFee() {
        return fee;
    }

    public void setFee(String fee) {
        this.fee = fee;
    }

    @Override
    public String toString() {
        return "TransInfo{" +
                "timeStamp='" + timeStamp + '\'' +
                ", userId='" + userId + '\'' +
                ", cardId='" + cardId + '\'' +
                ", transfer='" + transfer + '\'' +
                ", fee='" + fee + '\'' +
                '}';
    }
}
