package com.happy.connectors.kafka.event;

/**
 * @author happy
 * @since 2022/6/19
 */
public class KafkaEvent {
    private Integer id;
    private String name;

    public KafkaEvent() {
    }

    public KafkaEvent(Integer id, String name) {
        this.id = id;
        this.name = name;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "KafkaEvent{" +
                "id=" + id +
                ", name='" + name + '\'' +
                '}';
    }
}
