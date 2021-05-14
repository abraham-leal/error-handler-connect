package com.aleal.kafka.connect;

import org.apache.kafka.common.header.Headers;

import java.nio.charset.StandardCharsets;

public class ErrorHandlerSinkUtils {
    protected static Headers updateRetries (Headers recordHeaders , int currentRetries) {
        if (currentRetries == -1) { // relay provenance header setting
            recordHeaders.add("relay_retries", "1".getBytes(StandardCharsets.UTF_8));
        } else { // update retries
            recordHeaders.remove("relay_retries");
            recordHeaders.add("relay_retries", String.valueOf(currentRetries + 1).getBytes(StandardCharsets.UTF_8));
        }
        return recordHeaders;
    }
}
