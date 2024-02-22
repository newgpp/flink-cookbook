package com.felix.entity;

public class UserClick {

    private String userId;

    private Long ts;

    public UserClick() {
    }

    public UserClick(String userId, Long ts) {
        this.userId = userId;
        this.ts = ts;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "UserClick{" +
                "userId='" + userId + '\'' +
                ", ts=" + ts +
                '}';
    }
}
