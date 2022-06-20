package com.flink.cep.entity;

import java.util.Objects;

public class LoginEvent {

    private int userId;
    private String userName;
    private String loginStatus;
    private Long loginTime;

    public LoginEvent() {
    }

    public LoginEvent(int userId, String userName, String loginStatus, Long loginTime) {
        this.userId = userId;
        this.userName = userName;
        this.loginStatus = loginStatus;
        this.loginTime = loginTime;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getLoginStatus() {
        return loginStatus;
    }

    public void setLoginStatus(String loginStatus) {
        this.loginStatus = loginStatus;
    }

    public Long getLoginTime() {
        return loginTime;
    }

    public void setLoginTime(Long loginTime) {
        this.loginTime = loginTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LoginEvent that = (LoginEvent) o;
        return userId == that.userId && Objects.equals(userName, that.userName) && Objects.equals(loginStatus, that.loginStatus) && Objects.equals(loginTime, that.loginTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId, userName, loginStatus, loginTime);
    }

    @Override
    public String toString() {
        return "LoginEvent{" +
                "userId=" + userId +
                ", userName='" + userName + '\'' +
                ", loginStatus='" + loginStatus + '\'' +
                ", loginTime=" + loginTime +
                '}';
    }
}