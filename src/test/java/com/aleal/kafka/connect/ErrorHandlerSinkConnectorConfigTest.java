package com.aleal.kafka.connect;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;


public class ErrorHandlerSinkConnectorConfigTest {

    @Test
    public void testBadConfigIfRemapWithNoDestination () {
        Map<String, String> configs = new HashMap<>();
        configs.put("relayer.bootstrap.servers", "localhost:9092");
        configs.put("mode", "remap");
        configs.put("topics", "sample");

        assertThrows(ConfigException.class, ()-> new ErrorHandlerSinkConnectorConfig(configs));
    }

    @Test
    public void testBadConfigIfTopicsListsDontMatch () {
        Map<String, String> configs = new HashMap<>();
        configs.put("relayer.bootstrap.servers", "localhost:9092");
        configs.put("mode", "remap");
        configs.put("topics", "sample,sample2");
        configs.put("topics.destinations", "sampleDest");

        assertThrows(ConfigException.class, ()-> new ErrorHandlerSinkConnectorConfig(configs));
    }

    @Test
    public void testBadConfigIfConsumingFromFinalTopic () {
        Map<String, String> configs = new HashMap<>();
        configs.put("relayer.bootstrap.servers", "localhost:9092");
        configs.put("mode", "remap");
        configs.put("topics", "sample,sample2");
        configs.put("final.topic.no.retries", "sample");

        assertThrows(ConfigException.class, ()-> new ErrorHandlerSinkConnectorConfig(configs));
    }
}
