package com.entities;

import com.azure.cosmos.test.faultinjection.FaultInjectionConnectionErrorType;
import com.azure.cosmos.test.faultinjection.FaultInjectionConnectionType;
import com.azure.cosmos.test.faultinjection.FaultInjectionServerErrorType;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

@JsonDeserialize(using = FaultInjectionParametersDeserializer.class)
public class FaultInjectionParameters {

    private String region;

    private List<String> base64EncodedFeedRanges;

    private FaultInjectionConnectionType connectionType;

    private Duration faultInjectionDuration;

    private Duration connectionOrResponseDelayDuration;

    private FaultInjectionServerErrorType serverErrorType;

    private FaultInjectionConnectionErrorType connectionErrorType;

    public FaultInjectionParameters() {
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public List<String> getBase64EncodedFeedRanges() {
        return base64EncodedFeedRanges;
    }

    public void setBase64EncodedFeedRanges(List<String> base64EncodedFeedRanges) {
        this.base64EncodedFeedRanges = base64EncodedFeedRanges;
    }

    public FaultInjectionConnectionType getConnectionType() {
        return connectionType;
    }

    public void setConnectionType(FaultInjectionConnectionType connectionType) {
        this.connectionType = connectionType;
    }

    public Duration getFaultInjectionDuration() {
        return faultInjectionDuration;
    }

    public void setFaultInjectionDuration(Duration faultInjectionDuration) {
        this.faultInjectionDuration = faultInjectionDuration;
    }

    public Duration getConnectionOrResponseDelayDuration() {
        return connectionOrResponseDelayDuration;
    }

    public void setConnectionOrResponseDelayDuration(Duration connectionOrResponseDelayDuration) {
        this.connectionOrResponseDelayDuration = connectionOrResponseDelayDuration;
    }

    public FaultInjectionServerErrorType getServerErrorType() {
        return serverErrorType;
    }

    public void setServerErrorType(FaultInjectionServerErrorType serverErrorType) {
        this.serverErrorType = serverErrorType;
    }

    public FaultInjectionConnectionErrorType getConnectionErrorType() {
        return connectionErrorType;
    }

    public void setConnectionErrorType(FaultInjectionConnectionErrorType connectionErrorType) {
        this.connectionErrorType = connectionErrorType;
    }
}
