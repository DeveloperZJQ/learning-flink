package com.flink.datastream;

import java.util.Date;

/**
 * @author DeveloperZJQ
 * @since 2022-6-2
 */
public class ProcessInfo {
    private String compId;
    private String transId;
    private String processCd;
    private String processSuccessCd;
    private Integer retryCount;
    private Date processTime;

    public ProcessInfo(String compId, String transId, String processCd, String processSuccessCd, Integer retryCount, Date processTime) {
        this.compId = compId;
        this.transId = transId;
        this.processCd = processCd;
        this.processSuccessCd = processSuccessCd;
        this.retryCount = retryCount;
        this.processTime = processTime;
    }

    public String getCompId() {
        return compId;
    }

    public void setCompId(String compId) {
        this.compId = compId;
    }

    public String getTransId() {
        return transId;
    }

    public void setTransId(String transId) {
        this.transId = transId;
    }

    public String getProcessCd() {
        return processCd;
    }

    public void setProcessCd(String processCd) {
        this.processCd = processCd;
    }

    public String getProcessSuccessCd() {
        return processSuccessCd;
    }

    public void setProcessSuccessCd(String processSuccessCd) {
        this.processSuccessCd = processSuccessCd;
    }

    public Integer getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(Integer retryCount) {
        this.retryCount = retryCount;
    }

    public Date getProcessTime() {
        return processTime;
    }

    public void setProcessTime(Date processTime) {
        this.processTime = processTime;
    }

    @Override
    public String toString() {
        return "{" +
                "\"compId\":\"" + compId + '\"' +
                ",\"transId\":\"" + transId + '\"' +
                ",\"processCd\":\"" + processCd + '\"' +
                ",\"processSuccessCd\":\"" + processSuccessCd + '\"' +
                ",\"retryCount\":" + retryCount +
                ",\"processTime\":\"" + processTime +'\"'+
                "}";
    }

    public static void main(String[] args) {
        ProcessInfo processInfo = new ProcessInfo("1", "2", "3", "4", 5, new Date());
        System.out.println(processInfo.toString());
    }
}
