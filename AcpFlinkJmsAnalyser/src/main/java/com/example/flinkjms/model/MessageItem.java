package com.example.flinkjms.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;

public class MessageItem implements Serializable {
    
    @JsonProperty("id")
    private String id;
    
    @JsonProperty("value")
    private Double value;
    
    @JsonProperty("timestamp")
    private Long timestamp;
    
    @JsonProperty("metadata")
    private String metadata;
    
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getMetadata() {
        return metadata;
    }

    public void setMetadata(String metadata) {
        this.metadata = metadata;
    }

    public Instant getInstant() {
        return timestamp != null ? Instant.ofEpochMilli(timestamp) : Instant.now();
    }
    
    public boolean isValid() {
        return value != null && value <= 100;
    }
    
    public boolean isError() {
        return value != null && value > 100;
    }

    public MessageItem() {}

    public MessageItem(String id, Double value, Long timestamp, String metadata) {
        this.id = id;
        this.value = value;
        this.timestamp = timestamp;
        this.metadata = metadata;
    }
}
