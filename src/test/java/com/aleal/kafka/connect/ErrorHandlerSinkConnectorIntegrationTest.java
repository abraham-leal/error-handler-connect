package com.aleal.kafka.connect;

import org.junit.ClassRule;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

public class ErrorHandlerSinkConnectorIntegrationTest {

  @ClassRule
  public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.1.1"));

  @ClassRule
  public GenericContainer kconnect = new GenericContainer(DockerImageName.parse("confluentinc/cp-connect:6.1.1"))
          .dependsOn(kafka)
          .withExposedPorts(8083);

  @Test
  public void testCorrectRemap () {

  }
}
