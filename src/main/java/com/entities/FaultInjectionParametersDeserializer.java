package com.entities;

import com.azure.cosmos.test.faultinjection.FaultInjectionConnectionErrorType;
import com.azure.cosmos.test.faultinjection.FaultInjectionConnectionType;
import com.azure.cosmos.test.faultinjection.FaultInjectionServerErrorType;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FaultInjectionParametersDeserializer extends StdDeserializer<FaultInjectionParameters> {

    protected FaultInjectionParametersDeserializer(Class<FaultInjectionParameters> vc) {
        super(vc);
    }

    public FaultInjectionParametersDeserializer() {
        super(FaultInjectionParameters.class);
    }

    @Override
    public FaultInjectionParameters deserialize(JsonParser p, DeserializationContext ctx) throws IOException, JacksonException {

        JsonNode jsonNode = p.getCodec().readTree(p);

        String region = jsonNode.get("region").asText();

        List<String> feedRanges = new ArrayList<>();

        Iterator<JsonNode> feedRangeIterator = jsonNode.get("feedRanges").elements();

        while (feedRangeIterator.hasNext()) {
            JsonNode feedRange = feedRangeIterator.next();
            feedRanges.add(feedRange.asText());
        }

        String connectionTypeAsString = jsonNode.get("connectionType").asText();
        FaultInjectionConnectionType faultInjectionConnectionType = FaultInjectionConnectionType.valueOf(connectionTypeAsString);

        String faultInjectionDurationAsString = jsonNode.get("faultInjectionDuration").asText();
        String delayAsString = jsonNode.get("delay").asText();

        Duration faultInjectionDuration, responseDelayOrConnectionDelay;

        if ("-infinite".equalsIgnoreCase(faultInjectionDurationAsString)) {
            faultInjectionDuration = Duration.ZERO;
        } else {
            faultInjectionDuration = Duration.parse(faultInjectionDurationAsString);
        }

        if ("-infinite".equalsIgnoreCase(delayAsString)) {
            responseDelayOrConnectionDelay = Duration.ZERO;
        } else {
            responseDelayOrConnectionDelay = Duration.parse(delayAsString);
        }

        FaultInjectionServerErrorType faultInjectionServerErrorType = null;
        FaultInjectionConnectionErrorType faultInjectionConnectionErrorType = null;

        boolean isServerError = jsonNode.get("isServerError").asBoolean();
        String errorType = jsonNode.get("errorType").asText();

        if (isServerError) {

            switch (errorType) {

                case "INTERNAL_SERVER_ERROR":
                    faultInjectionServerErrorType = FaultInjectionServerErrorType.INTERNAL_SERVER_ERROR;
                    break;
                case "SERVICE_UNAVAILABLE":
                    faultInjectionServerErrorType = FaultInjectionServerErrorType.SERVICE_UNAVAILABLE;
                    break;
                case "GONE":
                    faultInjectionServerErrorType = FaultInjectionServerErrorType.GONE;
                    break;
                case "PARTITION_IS_MIGRATING":
                    faultInjectionServerErrorType = FaultInjectionServerErrorType.PARTITION_IS_MIGRATING;
                    break;
                case "PARTITION_IS_SPLITTING":
                    faultInjectionServerErrorType = FaultInjectionServerErrorType.PARTITION_IS_SPLITTING;
                    break;
                case "CONNECTION_DELAY":
                    faultInjectionServerErrorType = FaultInjectionServerErrorType.CONNECTION_DELAY;
                    break;
                case "RESPONSE_DELAY":
                    faultInjectionServerErrorType = FaultInjectionServerErrorType.RESPONSE_DELAY;
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported error type: " + errorType);
            }
        } else {
            switch (errorType) {
                case "CONNECTION_CLOSE":
                    faultInjectionConnectionErrorType = FaultInjectionConnectionErrorType.CONNECTION_CLOSE;
                    break;
                case "CONNECTION_REFUSED":
                    faultInjectionConnectionErrorType = FaultInjectionConnectionErrorType.CONNECTION_RESET;
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported error type: " + errorType);
            }
        }

        FaultInjectionParameters faultInjectionParameters = new FaultInjectionParameters();

        faultInjectionParameters.setRegion(region);
        faultInjectionParameters.setBase64EncodedFeedRanges(feedRanges);
        faultInjectionParameters.setConnectionType(faultInjectionConnectionType);
        faultInjectionParameters.setFaultInjectionDuration(faultInjectionDuration);
        faultInjectionParameters.setConnectionOrResponseDelayDuration(responseDelayOrConnectionDelay);
        faultInjectionParameters.setConnectionErrorType(faultInjectionConnectionErrorType);
        faultInjectionParameters.setServerErrorType(faultInjectionServerErrorType);

        return faultInjectionParameters;
    }
}
