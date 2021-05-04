package com.aleal.kafka.connect;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ErrorHandlerSinkConnectorTest {
  @Test
  public void testGetCorrectVersion() {
    ErrorHandlerSinkConnector thisConnector = new ErrorHandlerSinkConnector();
    assertEquals("1.0-SNAPSHOT", thisConnector.version());
  }

  @Test
  public void testGetCorrectTaskClass() {
    ErrorHandlerSinkConnector thisConnector = new ErrorHandlerSinkConnector();
    assertEquals(ErrorHandlerSinkTask.class, thisConnector.taskClass());
  }

  @Test
  public void testGetCorrectConfigDef() {
    ErrorHandlerSinkConnector thisConnector = new ErrorHandlerSinkConnector();
    assertEquals(ErrorHandlerSinkConnectorConfig.config().getClass(), thisConnector.config().getClass());
  }
}