package org.apache.flink.client.reader;

import java.io.Serializable;

public class Location implements Serializable {
    private Long id;
    private String name;
    private Long parent_id;
    private int type;
    private String country_code;

    public Location() {
    }

    public Location(Long id, String name, Long parent_id, int type, String country_code) {
        this.id = id;
        this.name = name;
        this.parent_id = parent_id;
        this.type = type;
        this.country_code = country_code;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getParent_id() {
        return parent_id;
    }

    public void setParent_id(Long parent_id) {
        this.parent_id = parent_id;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public String getCountry_code() {
        return country_code;
    }

    public void setCountry_code(String country_code) {
        this.country_code = country_code;
    }

    @Override
    public String toString() {
        return "Location{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", parent_id=" + parent_id +
                ", type=" + type +
                ", country_code='" + country_code + '\'' +
                '}';
    }
}