package com.aleal.kafka.connect;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import static org.junit.jupiter.api.Assertions.*;

public class ErrorHandlerSinkConnectorIntegrationTest {
  private static Logger log = LoggerFactory.getLogger(ErrorHandlerSinkConnectorIntegrationTest.class);

  static Network env = Network.newNetwork();
  private static final String sourceTopic = "someRealSource";
  private static final String dlqTopic = "someErrorSource";
  private static final String mappedToTopic = "someDestination";
  private static final String sampleKey = "someKey";
  private static final String sampleValue = "someValue";


  @ClassRule
  public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.1.1"))
          .withNetwork(env);

  protected Map<String, String> getConnectConfigs() {
    Map<String,String> connectConfigs = new HashMap<>();
    connectConfigs.put("CONNECT_BOOTSTRAP_SERVERS", kafka.getNetworkAliases().get(0)+":9092");
    connectConfigs.put("CONNECT_REST_ADVERTISED_HOST_NAME","connect");
    connectConfigs.put("CONNECT_REST_PORT","8083");
    connectConfigs.put("CONNECT_GROUP_ID","compose-connect-group");
    connectConfigs.put("CONNECT_CONFIG_STORAGE_TOPIC","docker-connect-configs");
    connectConfigs.put("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR","1");
    connectConfigs.put("CONNECT_OFFSET_STORAGE_TOPIC","docker-connect-offsets");
    connectConfigs.put("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR","1");
    connectConfigs.put("CONNECT_STATUS_STORAGE_TOPIC","docker-connect-status");
    connectConfigs.put("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR","1");
    connectConfigs.put("CONNECT_KEY_CONVERTER","org.apache.kafka.connect.json.JsonConverter");
    connectConfigs.put("CONNECT_VALUE_CONVERTER","org.apache.kafka.connect.json.JsonConverter");

    return connectConfigs;
  }

  @ClassRule
  @SuppressWarnings({"unchecked"})
  public GenericContainer kconnect = new GenericContainer(DockerImageName.parse("abrahamleal/cp-kafka-connect-error-handler:1.0"))
          .withNetwork(env)
          .dependsOn(kafka)
          .withImagePullPolicy(PullPolicy.alwaysPull())
          .withExposedPorts(8083)
          .waitingFor(Wait.forHttp("/"));

  //
  @Test
  public void testCorrectRemap () {
    ErrorHandlerSinkConnectorIntegrationTestUtils testUtils = startResources();

    // Connector will listen to the dlq topic, and map to the mappedToTopic
    List<String> sourceTopics = new ArrayList<>();
    sourceTopics.add(dlqTopic);
    List<String> destTopics = new ArrayList<>();
    destTopics.add(mappedToTopic);

    try {
      // Post Connector
      testUtils.postErrorHandlingConnector(ErrorHandlerSinkTask.Mode.REMAP.toString(), sourceTopics, destTopics);
    } catch (IOException e) {
      log.error("Could not post connector: {}", e.getLocalizedMessage());
      fail();
    }

    try {
      // Create the a real source along with the various topics
      sourceTopics.add(sourceTopic);
      testUtils.ensureTopicsExist(sourceTopics);
      testUtils.ensureTopicsExist(destTopics);
    } catch (Exception e) {
      log.error("Could not ensure creation of source|destination topics: {}", e.getLocalizedMessage());
    }

    RecordMetadata originalRecord = null;
    ConsumerRecord<String, String> relayedRecord = null;

    // Round-trip a record through the connector
    try {
      // Produce a fake "real" record
      originalRecord = testUtils.produce(sourceTopic, sampleKey, sampleValue);
      // Produce fake error report
      testUtils.produceError(dlqTopic, sourceTopic, originalRecord.partition(), originalRecord.offset(), sampleKey, sampleValue);
    } catch (ExecutionException | InterruptedException i) {
      log.error("Production was unsuccessful: {}", i.getLocalizedMessage());
    }

    try {
      testUtils.waitForTopicPartitionToPopulate(mappedToTopic, 0, 60000);
      relayedRecord = testUtils.consume(mappedToTopic, 0, 0);
    } catch (ExecutionException | InterruptedException i) {
      log.error("Could not wait for partition population due to {}", i.getLocalizedMessage());
    }

    if (relayedRecord != null) {
      assertEquals(originalRecord.partition(), relayedRecord.partition());
      assertEquals(sampleKey, relayedRecord.key());
      assertEquals(sampleValue, relayedRecord.value());
    } else {
      fail();
    }

    stopResources();

  }

  private ErrorHandlerSinkConnectorIntegrationTestUtils startResources() {
    kafka.start();
    kconnect.withEnv(getConnectConfigs()).start();

    Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(log);
    kconnect.followOutput(logConsumer);

    ErrorHandlerSinkConnectorIntegrationTestUtils testUtils = new ErrorHandlerSinkConnectorIntegrationTestUtils(
            String.format("%s:%d",kafka.getNetworkAliases().get(0),9092),
            kafka.getBootstrapServers(),
            String.format("%s:%d",kconnect.getHost(),kconnect.getFirstMappedPort()));

    List<String> connectTopics = new ArrayList<>();
    connectTopics.add("docker-connect-offsets");
    connectTopics.add("docker-connect-config");
    connectTopics.add("docker-connect-status");
    try {
      testUtils.ensureTopicsExist(connectTopics);
    } catch (Exception e) {
      log.error("Could not ensure creation of connect topics.");
    }

    return testUtils;
  }

  private void stopResources() {
    kafka.stop();
    kconnect.stop();
  }

}
