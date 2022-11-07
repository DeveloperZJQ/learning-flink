package org.apache.flink.client.reader;

import java.util.List;

public class Country {
    private String couId;
    private String cc2;
    private List<Province> states;
    private String name;

    public String getCouId() {
        return couId;
    }

    public void setCouId(String couId) {
        this.couId = couId;
    }

    public String getCc2() {
        return cc2;
    }

    public void setCc2(String cc2) {
        this.cc2 = cc2;
    }

    public List<Province> getStates() {
        return states;
    }

    public void setStates(List<Province> states) {
        this.states = states;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
