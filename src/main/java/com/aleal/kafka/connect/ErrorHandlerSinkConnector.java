package com.aleal.kafka.connect;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
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
}
