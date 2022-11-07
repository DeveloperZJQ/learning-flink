package org.apache.flink.client.reader;

import java.util.List;

public class Province {
    private List<City> cities;
    private String sttId;
    private String name;
    private String couId;

    public List<City> getCities() {
        return cities;
    }

    public void setCities(List<City> cities) {
        this.cities = cities;
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

    public String getCouId() {
        return couId;
    }

    public void setCouId(String couId) {
        this.couId = couId;
    }
}