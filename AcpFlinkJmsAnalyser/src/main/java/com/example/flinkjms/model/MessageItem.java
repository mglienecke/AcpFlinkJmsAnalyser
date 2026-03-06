package com.example.flinkjms.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

import java.io.Serializable;
import java.time.Instant;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class MessageItem implements Serializable {

    private static final long serialVersionUID = 1L;
    
    @JsonProperty("id")
    private String id;
    
    @JsonProperty("value")
    private Double value;
    
    @JsonProperty("timestamp")
    private Long timestamp;
    
    @JsonProperty("metadata")
    private String metadata;

    @JsonIgnore
    public Instant getInstant() {
        return timestamp != null ? Instant.ofEpochMilli(timestamp) : Instant.now();
    }

    @JsonIgnore
    public boolean isValid() {
        return value != null && value <= 100;
    }

    @JsonIgnore
    public boolean isError() {
        return value != null && value > 100;
    }
}
