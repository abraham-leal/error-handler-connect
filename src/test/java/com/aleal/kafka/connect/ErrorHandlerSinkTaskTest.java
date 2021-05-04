package com.aleal.kafka.connect;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

public class ErrorHandlerSinkTaskTest {
  @Test
  public void testCorrectStart() {

    // Minimum config for remapping error handler
    Map<String, String> configs = new HashMap<>();
    configs.put("relayer.bootstrap.servers", "localhost:9092");
    configs.put("mode", "remap");
    configs.put("topics", "sample");
    configs.put("topics.destinations", "sampleDest");

    // Expected mapping for topics
    Map<String,String> topicMappings = new HashMap<>();
    topicMappings.put("sample", "sampleDest");

    // Mock Error Handler Task
    ErrorHandlerSinkTask myTask = new ErrorHandlerSinkTask();
    myTask.start(configs);

    // Test
    assertNotNull(myTask.replayProducer);
    assertNotNull(myTask.seekingConsumer);
    assertEquals(ErrorHandlerSinkTask.Mode.REMAP, myTask.mode);
    assertEquals(topicMappings, myTask.srcDestTopicMapping);
    assertEquals(ErrorHandlerSinkConnectorConfig.NO_RETRIES_TOPIC_DEFAULT, myTask.noRetriesTopic);
  }


}
