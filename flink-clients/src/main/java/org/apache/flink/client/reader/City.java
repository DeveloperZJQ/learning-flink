package org.apache.flink.client.reader;

public class City {
    private String citId;
    private String sttId;
    private String name;

    public String getCitId() {
        return citId;
    }

    public void setCitId(String citId) {
        this.citId = citId;
    }

    public String getSttId() {
        return sttId;
    }

    public void setSttId(String sttId) {
        this.sttId = sttId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}