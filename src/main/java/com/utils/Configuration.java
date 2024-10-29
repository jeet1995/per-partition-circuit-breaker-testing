package com.utils;

import com.azure.cosmos.ConnectionMode;
import com.azure.cosmos.implementation.TestConfigurations;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;

import java.time.Duration;
import java.util.Locale;

public class Configuration {

    @Parameter(names = "-serviceEndpoint", description = "The service endpoint.")
    private String serviceEndpoint = TestConfigurations.HOST;

    @Parameter(names = "-masterKey", description = "The master key for the account.")
    private String masterKey = TestConfigurations.MASTER_KEY;

    @Parameter(names = "-databaseName", description = "The database name.")
    private String databaseName = "circuitBreakerTestDb";

    @Parameter(names = "-containerName", description = "The container name.")
    private String containerName = "circuitBreakerTestContainer";

    @Parameter(names = "-connectionMode", description = "The connectivity mode - DIRECT / GATEWAY.", converter = ConnectionModeConverter.class)
    private ConnectionMode connectionMode = ConnectionMode.DIRECT;

    @Parameter(names = "-isThresholdBasedAvailabilityStrategyEnabled", description = "A boolean flag which indicates whether threshold based availability strategy is enabled.", arity = 1)
    private boolean isThresholdBasedAvailabilityStrategyEnabled = false;

    @Parameter(names = "-preferredRegions", description = "Comma separated preferred regions list.")
    private String commaSeparatedPreferredRegions = "East US, South Central US";

    @Parameter(names = "-faultInjectionPeriodicity", description = "The time interval at which a certain set of faults will be injected.", converter = DurationConverter.class)
    private Duration faultInjectionPeriodicity = Duration.ofMinutes(2);

    @Parameter(names = "-runDuration", description = "The duration for which the workload should run.", converter = DurationConverter.class)
    private Duration runDuration = Duration.ofMinutes(5);

    @Parameter(names = "-pointOperationEndToEndTimeout", description = "The end-to-end operation timeout for a point operation.", converter = DurationConverter.class)
    private Duration pointOperationEndToEndTimeout = Duration.ofSeconds(3);

    @Parameter(names = "-itemCountToPreCreate", description = "The count of items to create right after creating the container.")
    private int itemCountToPreCreate = 100;

    @Parameter(names = "-operationTaskCount", description = "The count of tasks used to run the operation workload.")
    private int operationTaskCount = 5;

    @Parameter(names = "-containerManualProvisionedThroughput", description = "The manual provisioned throughput to be set on the container if not already created.")
    private int containerManualProvisionedThroughput = 12_000;

    @Parameter(names = "-faultInjectionPayloadId", description = "The id of the fault injection payload which encapsulates fault settings.")
    private String faultInjectionPayloadId = "partition_gone_inject_type_1.json";

    public String getServiceEndpoint() {
        return serviceEndpoint;
    }

    public Configuration setServiceEndpoint(String serviceEndpoint) {
        this.serviceEndpoint = serviceEndpoint;
        return this;
    }

    public Duration getPointOperationEndToEndTimeout() {
        return pointOperationEndToEndTimeout;
    }

    public Configuration setPointOperationEndToEndTimeout(Duration pointOperationEndToEndTimeout) {
        this.pointOperationEndToEndTimeout = pointOperationEndToEndTimeout;
        return this;
    }

    public Duration getRunDuration() {
        return this.runDuration;
    }

    public Configuration setRunDuration(Duration runDuration) {
        this.runDuration = runDuration;
        return this;
    }

    public Duration getFaultInjectionPeriodicity() {
        return faultInjectionPeriodicity;
    }

    public Configuration setFaultInjectionPeriodicity(Duration faultInjectionPeriodicity) {
        this.faultInjectionPeriodicity = faultInjectionPeriodicity;
        return this;
    }

    public String getCommaSeparatedPreferredRegions() {
        return commaSeparatedPreferredRegions;
    }

    public Configuration setCommaSeparatedPreferredRegions(String commaSeparatedPreferredRegions) {
        this.commaSeparatedPreferredRegions = commaSeparatedPreferredRegions;
        return this;
    }

    public boolean isThresholdBasedAvailabilityStrategyEnabled() {
        return isThresholdBasedAvailabilityStrategyEnabled;
    }

    public Configuration setThresholdBasedAvailabilityStrategyEnabled(boolean thresholdBasedAvailabilityStrategyEnabled) {
        isThresholdBasedAvailabilityStrategyEnabled = thresholdBasedAvailabilityStrategyEnabled;
        return this;
    }

    public ConnectionMode getConnectionMode() {
        return connectionMode;
    }

    public Configuration setConnectionMode(ConnectionMode connectionMode) {
        this.connectionMode = connectionMode;
        return this;
    }

    public String getContainerName() {
        return containerName;
    }

    public Configuration setContainerName(String containerName) {
        this.containerName = containerName;
        return this;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public Configuration setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
        return this;
    }

    public String getMasterKey() {
        return masterKey;
    }

    public Configuration setMasterKey(String masterKey) {
        this.masterKey = masterKey;
        return this;
    }

    public int getItemCountToPreCreate() {
        return itemCountToPreCreate;
    }

    public Configuration setItemCountToPreCreate(int itemCountToPreCreate) {
        this.itemCountToPreCreate = itemCountToPreCreate;
        return this;
    }

    public int getOperationTaskCount() {
        return operationTaskCount;
    }

    public Configuration setOperationTaskCount(int operationTaskCount) {
        this.operationTaskCount = operationTaskCount;
        return this;
    }

    public int getContainerManualProvisionedThroughput() {
        return containerManualProvisionedThroughput;
    }

    public Configuration setContainerManualProvisionedThroughput(int containerManualProvisionedThroughput) {
        this.containerManualProvisionedThroughput = containerManualProvisionedThroughput;
        return this;
    }

    public String getFaultInjectionPayloadId() {
        return faultInjectionPayloadId;
    }

    public Configuration setFaultInjectionPayloadId(String faultInjectionPayloadId) {
        this.faultInjectionPayloadId = faultInjectionPayloadId;
        return this;
    }

    static class DurationConverter implements IStringConverter<Duration> {
        @Override
        public Duration convert(String value) {
            if (value == null) {
                return null;
            }

            return Duration.parse(value);
        }
    }

    static class ConnectionModeConverter implements IStringConverter<ConnectionMode> {

        @Override
        public ConnectionMode convert(String value) {
            String normalizedConnectionModeAsString
                    = value.toLowerCase(Locale.ROOT).replace(" ", "").trim();

            ConnectionMode result;

            if (normalizedConnectionModeAsString.equals("gateway")) {
                result = ConnectionMode.GATEWAY;
            } else {
                result = ConnectionMode.DIRECT;
            }

            return result;
        }
    }
}
