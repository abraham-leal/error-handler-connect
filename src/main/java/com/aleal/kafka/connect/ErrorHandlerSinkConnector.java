package com.aleal.kafka.connect;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErrorHandlerSinkConnector extends SinkConnector {
  private static Logger log = LoggerFactory.getLogger(ErrorHandlerSinkConnector.class);
  private Map<String, String> config;

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    ArrayList<Map<String, String>> configs = new ArrayList<>();
    for (int i = 0; i < maxTasks; i++) {
      configs.add(config);
    }
    return configs;
  }

  @Override
  public void start(Map<String, String> settings) {
    this.config = settings;
  }


  @Override
  public void stop() {
  }

  @Override
  public ConfigDef config() {
    return ErrorHandlerSinkConnectorConfig.config();
  }

  @Override
  public Class<? extends Task> taskClass() {
    return ErrorHandlerSinkTask.class;
  }

  @Override
  public String version() { return "1.0-SNAPSHOT"; }

  @Override
  public Config validate(final Map<String, String> connectorConfigs) {
    Config config = super.validate(connectorConfigs);

    String mode = connectorConfigs.get(ErrorHandlerSinkConnectorConfig.MODE_CONFIG);
    List<String> topics = Arrays.asList(connectorConfigs.get(ErrorHandlerSinkConnectorConfig.TOPICS_CONFIG).split(","));
    List<String> destTopics = Arrays.asList(connectorConfigs.get(ErrorHandlerSinkConnectorConfig.TOPICS_DEST_CONFIG).split(","));

    if (mode.equalsIgnoreCase("resend")) {
      if (topics.size() == 0) {
        throw new ConfigException("Cannot use resend mode without a source topic list");
      }
    }

    if (mode.equalsIgnoreCase("remap")) {
      if (destTopics.size() == 0 || topics.size() == 0) {
        throw new ConfigException("Cannot use remap mode without topics and topics.destinations settings defined.");
      }
      if (topics.size() != destTopics.size()) {
        throw new ConfigException("Cannot use remap mode without having an equal amount of topic destinations as topics" +
                "to consume from");
      }
    }

    if (topics.contains(connectorConfigs.get(ErrorHandlerSinkConnectorConfig.NO_RETRIES_TOPIC_CONFIG))) {
      throw new ConfigException("Consuming from the configured final topic is not supported.");
    }

    return config;
  }
}
