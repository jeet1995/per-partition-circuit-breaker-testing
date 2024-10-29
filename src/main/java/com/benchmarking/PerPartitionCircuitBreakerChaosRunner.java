package com.benchmarking;

import com.azure.cosmos.ConnectionMode;
import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosDiagnosticsContext;
import com.azure.cosmos.CosmosEndToEndOperationLatencyPolicyConfigBuilder;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.ThresholdBasedAvailabilityStrategy;
import com.azure.cosmos.implementation.ServiceUnavailableException;
import com.azure.cosmos.implementation.directconnectivity.GatewayAddressCache;
import com.azure.cosmos.models.CosmosBulkOperations;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosItemOperation;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.FeedRange;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.cosmos.test.faultinjection.CosmosFaultInjectionHelper;
import com.azure.cosmos.test.faultinjection.FaultInjectionCondition;
import com.azure.cosmos.test.faultinjection.FaultInjectionConditionBuilder;
import com.azure.cosmos.test.faultinjection.FaultInjectionEndpointBuilder;
import com.azure.cosmos.test.faultinjection.FaultInjectionResultBuilders;
import com.azure.cosmos.test.faultinjection.FaultInjectionRule;
import com.azure.cosmos.test.faultinjection.FaultInjectionRuleBuilder;
import com.azure.cosmos.test.faultinjection.FaultInjectionServerErrorResult;
import com.entities.FaultInjectionParameters;
import com.entities.Item;
import com.entities.ItemCount;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.utils.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class PerPartitionCircuitBreakerChaosRunner {

    private static final ScheduledThreadPoolExecutor GLOBAL_EXECUTOR = new ScheduledThreadPoolExecutor(1);

    private ScheduledFuture<?>[] scheduledFutures;

    private static final AtomicBoolean IS_STOPPED = new AtomicBoolean(false);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final Scheduler FAULT_INJECTION_SCHEDULER = Schedulers.newSingle("fault-injector-single");

    private static final Logger logger = LoggerFactory.getLogger(PerPartitionCircuitBreakerChaosRunner.class);

    // Steps to carry out:
    //  1. Create client
    //  2. Create database and container if not already exists
    //  3. Create some non-zero positive count of items in the container
    //  4. Schedule point reads at some fixed rate until some finite duration
    //  5. In parallel with 4, schedule fault injections to 1 or more feed ranges
    public void run(Configuration config) {
        setupGlobalExecutor();
        this.scheduledFutures = setupScheduledFutures(config);

        System.setProperty(
                "COSMOS.PARTITION_LEVEL_CIRCUIT_BREAKER_CONFIG",
                "{\"isPartitionLevelCircuitBreakerEnabled\": true, "
                        + "\"circuitBreakerType\": \"CONSECUTIVE_EXCEPTION_COUNT_BASED\","
                        + "\"consecutiveExceptionCountToleratedForReads\": 10,"
                        + "\"consecutiveExceptionCountToleratedForWrites\": 5,"
                        + "}");

        try (CosmosAsyncClient cosmosAsyncClient = buildCosmosAsyncClient(config)) {
            CosmosAsyncContainer cosmosAsyncContainer = setupCosmosServiceSideResources(config, cosmosAsyncClient);
            setupContainerWithDocuments(config, cosmosAsyncContainer);

            CosmosItemRequestOptions cosmosItemRequestOptionsForRead = buildCosmosItemRequestOptions(config);

            for (int i = 0; i < this.scheduledFutures.length; i++) {
                this.scheduledFutures[i] = GLOBAL_EXECUTOR.schedule(() -> {
                    try {
                        onReadItemLoop(cosmosAsyncContainer, cosmosItemRequestOptionsForRead, config);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }, 10, TimeUnit.MILLISECONDS);
            }

            Flux<Void> faultInjectorProcess = startFaultInjectorProcess(config, cosmosAsyncContainer);

            faultInjectorProcess.subscribe();

            Duration runDuration = config.getRunDuration();

            int oneSecondLoopCountRequired = (int) runDuration.getSeconds();

            logger.info("One second loop count required for workload run : {}", oneSecondLoopCountRequired);

            for (int i = 0; i < oneSecondLoopCountRequired; i++) {
                Thread.sleep(1_000);
            }

            IS_STOPPED.set(true);


        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {

            for (ScheduledFuture<?> scheduledFuture : this.scheduledFutures) {
                scheduledFuture.cancel(true);
            }

            GLOBAL_EXECUTOR.shutdown();
        }
    }

    private static void setupGlobalExecutor() {
        GLOBAL_EXECUTOR.setRemoveOnCancelPolicy(true);
        GLOBAL_EXECUTOR.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
    }

    private void onReadItemLoop(
            CosmosAsyncContainer cosmosAsyncContainer,
            CosmosItemRequestOptions readItemRequestOptions,
            Configuration config) throws InterruptedException {

        int itemCount = config.getItemCountToPreCreate();
        int loopIterations = Math.min(itemCount, 100);

        while (!IS_STOPPED.get()) {

            for (int i = 1; i <= loopIterations; i++) {

                String id = String.valueOf(i);
                String pk = String.valueOf(i);

                cosmosAsyncContainer
                        .readItem(id, new PartitionKey(pk), readItemRequestOptions, Item.class)
                        .onErrorComplete(throwable -> {
                            if (throwable instanceof CosmosException) {
                                CosmosException cosmosException = (CosmosException) throwable;

                                CosmosDiagnosticsContext ctx = cosmosException.getDiagnostics().getDiagnosticsContext();

                                logger.error("Diagnostics : {}", ctx.getDiagnostics());

                                logger.error("Error reading an item with status code : {} and sub-status code : {}",
                                        ctx.getStatusCode(),
                                        ctx.getSubStatusCode());

                                if (ctx.getContactedRegionNames() != null && !ctx.getContactedRegionNames().isEmpty()) {

                                    StringBuilder sb = new StringBuilder();

                                    ctx.getContactedRegionNames().forEach(s -> sb.append(s).append(","));

                                    logger.error("Error occurred when reading from regions : {}", sb);
                                }

                                return true;
                            }

                            IS_STOPPED.compareAndSet(false, true);
                            return false;
                        })
                        .block();
            }

            Thread.sleep(1_000);
        }
    }

    private static ScheduledFuture<?>[] setupScheduledFutures(Configuration config) {
        int taskCount = config.getOperationTaskCount();
        return new ScheduledFuture<?>[taskCount];
    }

    private static CosmosAsyncClient buildCosmosAsyncClient(Configuration config) {

        String endpoint = config.getServiceEndpoint();
        String masterKey = config.getMasterKey();
        List<String> preferredRegions = Arrays.asList(config.getCommaSeparatedPreferredRegions().split(","));
        ConnectionMode connectionMode = config.getConnectionMode();

        CosmosClientBuilder cosmosClientBuilder = new CosmosClientBuilder();

        cosmosClientBuilder = cosmosClientBuilder
                .endpoint(endpoint)
                .key(masterKey)
                .preferredRegions(Arrays.asList("East US", "South Central US"))
                .consistencyLevel(ConsistencyLevel.SESSION);

        if (connectionMode == ConnectionMode.DIRECT) {
            cosmosClientBuilder = cosmosClientBuilder.directMode();
        } else {
            cosmosClientBuilder = cosmosClientBuilder.gatewayMode();
        }

        return cosmosClientBuilder.buildAsyncClient();
    }

    private CosmosAsyncContainer setupCosmosServiceSideResources(Configuration config, CosmosAsyncClient cosmosAsyncClient) {
        int containerManualProvisionedThroughput = config.getContainerManualProvisionedThroughput();
        String containerId = config.getContainerName();
        String databaseId = config.getDatabaseName();

        cosmosAsyncClient.createDatabaseIfNotExists(databaseId).block();
        CosmosAsyncDatabase cosmosAsyncDatabase = cosmosAsyncClient.getDatabase(databaseId);

        CosmosContainerProperties cosmosContainerProperties = new CosmosContainerProperties(containerId, "/id");
        cosmosAsyncDatabase.createContainerIfNotExists(cosmosContainerProperties, ThroughputProperties.createManualThroughput(containerManualProvisionedThroughput)).block();

        return cosmosAsyncDatabase.getContainer(containerId);
    }

    private static void setupContainerWithDocuments(Configuration config, CosmosAsyncContainer cosmosAsyncContainer) {

        int itemCountToPreCreate = config.getItemCountToPreCreate();
        String itemCountDocPk = "itemCountDoc";

        CosmosItemResponse<ItemCount> itemCountResponse = cosmosAsyncContainer
                .readItem(
                        itemCountDocPk,
                        new PartitionKey(itemCountDocPk),
                        ItemCount.class)
                .onErrorComplete(throwable -> {
                    logger.error("Container with ID : {} is empty, will attempt to upsert {} documents.", config.getContainerName(), config.getItemCountToPreCreate());
                    return throwable instanceof CosmosException;
                })
                .block();

        try {

            ItemCount itemCount = itemCountResponse == null ? new ItemCount(itemCountDocPk, 0) : itemCountResponse.getItem();

            int count = itemCount == null ? 0 : itemCount.getCount();

            if (count < itemCountToPreCreate) {

                int start = count + 1;

                Flux<CosmosItemOperation> cosmosItemOperationFlux = Flux.range(start, itemCountToPreCreate)
                        .flatMap(integer -> {
                            String integerAsString = String.valueOf(integer);
                            Item itemToUpsert = new Item(integerAsString);
                            return Flux.just(CosmosBulkOperations.getUpsertItemOperation(itemToUpsert, new PartitionKey(itemToUpsert.getId())));
                        });

                cosmosAsyncContainer.executeBulkOperations(cosmosItemOperationFlux).blockLast();
                cosmosAsyncContainer.upsertItem(new ItemCount(itemCountDocPk, itemCountToPreCreate)).block();
            }

        } finally {
        }
    }

    private static Mono<Void> injectFault(Configuration config, CosmosAsyncContainer cosmosAsyncContainer) throws IOException {

        List<FaultInjectionParameters> faultInjectionParameters = loadFaultInjectionParametersFromFileIfExists(config);
        List<FaultInjectionRule> faultInjectionRules = new ArrayList<>();

        assert faultInjectionParameters != null;

        for (FaultInjectionParameters faultInjectionParameter : faultInjectionParameters) {
            FaultInjectionServerErrorResult faultInjectionInternalServerErrorResult = FaultInjectionResultBuilders
                    .getResultBuilder(faultInjectionParameter.getServerErrorType())
                    .build();

            List<String> base64EncodedFeedRanges = faultInjectionParameter.getBase64EncodedFeedRanges();

            for (String base64EncodedFeedRange : base64EncodedFeedRanges) {

                FeedRange feedRange = FeedRange.fromString(base64EncodedFeedRange);

                FaultInjectionCondition faultInjectionCondition = new FaultInjectionConditionBuilder()
                        .connectionType(faultInjectionParameter.getConnectionType())
                        .endpoints(new FaultInjectionEndpointBuilder(feedRange).build())
                        .region(faultInjectionParameter.getRegion())
                        .build();

                FaultInjectionRule faultInjectionInternalServerErrorRule = new FaultInjectionRuleBuilder("error-" + UUID.randomUUID())
                        .condition(faultInjectionCondition)
                        .result(faultInjectionInternalServerErrorResult)
                        .duration(faultInjectionParameter.getFaultInjectionDuration())
                        .build();

                faultInjectionRules.add(faultInjectionInternalServerErrorRule);
            }
        }

        return CosmosFaultInjectionHelper.configureFaultInjectionRules(cosmosAsyncContainer, faultInjectionRules);
    }

    private static Flux<Void> startFaultInjectorProcess(Configuration config, CosmosAsyncContainer cosmosAsyncContainer) {
        return Mono.just(1)
                .delayElement(config.getFaultInjectionPeriodicity())
                .publishOn(FAULT_INJECTION_SCHEDULER)
                .repeat(() -> !IS_STOPPED.get())
                .flatMap(ignore -> {
                        if (!IS_STOPPED.get()) {
                            logger.info("Attempting to inject faults defined in file : {} into container with ID : {}", config.getFaultInjectionPayloadId(), config.getContainerName());
                            try {
                                return injectFault(config, cosmosAsyncContainer);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        } else {
                            logger.error("Workload has ended!");
                            System.exit(1);
                        }
                    return null;
                })
                .onErrorComplete()
                .subscribeOn(FAULT_INJECTION_SCHEDULER);
    }

    private static CosmosItemRequestOptions buildCosmosItemRequestOptions(Configuration config) {
        CosmosItemRequestOptions cosmosItemRequestOptions = new CosmosItemRequestOptions();

        Duration endToEndOperationTimeout = config.getPointOperationEndToEndTimeout();

        CosmosEndToEndOperationLatencyPolicyConfigBuilder e2eLatencyPolicyCfgBuilder
                = new CosmosEndToEndOperationLatencyPolicyConfigBuilder(endToEndOperationTimeout);

        if (config.isThresholdBasedAvailabilityStrategyEnabled()) {
            e2eLatencyPolicyCfgBuilder = e2eLatencyPolicyCfgBuilder.availabilityStrategy(new ThresholdBasedAvailabilityStrategy());
        }

        cosmosItemRequestOptions.setCosmosEndToEndOperationLatencyPolicyConfig(e2eLatencyPolicyCfgBuilder.build());

        return cosmosItemRequestOptions;
    }

    private static List<FaultInjectionParameters> loadFaultInjectionParametersFromFileIfExists(Configuration config) throws IOException {
        String faultInjectionPayloadId = config.getFaultInjectionPayloadId();

        Path root = FileSystems.getDefault().getPath("").toAbsolutePath();
        Path targetPath = Paths.get(root.toString(), "fault-injection-payload", faultInjectionPayloadId);

        if (Files.exists(targetPath)) {
            try (InputStream in = Files.newInputStream(targetPath)) {

                byte[] allBytes = in.readAllBytes();

                return OBJECT_MAPPER.readValue(allBytes, new TypeReference<List<FaultInjectionParameters>>() {});
            }
        }

        return null;
    }
}
