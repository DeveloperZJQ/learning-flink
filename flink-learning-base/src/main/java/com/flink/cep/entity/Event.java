package com.flink.cep.entity;

import java.util.Date;
import java.util.Objects;

/**
 * @author lcx
 * @since 2020/11/8 18:06
 */
public class Event {

    private String name;

    /**
     * 事件类型
     */
    private Integer type;

    /**
     * 时间戳
     */
    private Long timestamp;


    private Date date;


    public Event() {
    }


    public Event(String name, Integer type, Long timestamp) {
        this.name = name;
        this.type = type;
        this.timestamp = timestamp;
    }

    public Event(String name, Integer type, Long timestamp, Date date) {
        this.name = name;
        this.type = type;
        this.timestamp = timestamp;
        this.date = date;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }


    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }


    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Event event = (Event) o;
        return Objects.equals(name, event.name) && Objects.equals(type, event.type) && Objects.equals(timestamp, event.timestamp) && Objects.equals(date, event.date);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, timestamp, date);
    }

    @Override
    public String toString() {
        return "Event{" +
                "name='" + name + '\'' +
                ", type=" + type +
                ", timestamp=" + timestamp +
                ", date=" + date +
                '}';
    }
}