package com.flink.datastream.entity;

/**
 * @author DeveloperZJQ
 * @since 2022-6-5
 */
public class Student {
    private Integer id;
    private String name;
    private Double score;
    private Long unixTime;

    public Student(Integer id, String name, Double score, Long unixTime) {
        this.id = id;
        this.name = name;
        this.score = score;
        this.unixTime = unixTime;
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

    public Double getScore() {
        return score;
    }

    public void setScore(Double score) {
        this.score = score;
    }

    public Long getUnixTime() {
        return unixTime;
    }

    public void setUnixTime(Long unixTime) {
        this.unixTime = unixTime;
    }
}
