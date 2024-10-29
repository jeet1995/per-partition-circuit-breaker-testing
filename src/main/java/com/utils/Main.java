package com.utils;

import com.benchmarking.PerPartitionCircuitBreakerChaosRunner;
import com.beust.jcommander.JCommander;

public class Main {
    public static void main(String[] args) {

        Configuration config = new Configuration();

        JCommander jCommander = new JCommander(config, null, args);

        PerPartitionCircuitBreakerChaosRunner perPartitionCircuitBreakerChaosRunner = new PerPartitionCircuitBreakerChaosRunner();
        perPartitionCircuitBreakerChaosRunner.run(config);
    }
}