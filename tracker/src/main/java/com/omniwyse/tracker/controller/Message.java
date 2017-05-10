package com.omniwyse.tracker.controller;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Message {
    @JsonProperty("data")
    private Object data;
    private String id;
    private String type;

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }
    public void setType(String type) {
        this.type = type;
    }
}
