package com.aleal.kafka.connect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigException;

import java.util.List;
import java.util.Map;
import java.util.Optional;


public class ErrorHandlerSinkConnectorConfig extends AbstractConfig {

  public static final String TOPICS_CONFIG = "topics";
  private static final String TOPICS_DOC =
          "A list of kafka topics for the sink connector, separated by commas";
  public static final String TOPICS_DEFAULT = "";
  private static final String TOPICS_DISPLAY = "The Kafka topics to watch";


  public static final String TOPICS_DEST_CONFIG = "topics.destinations";
  private static final String TOPICS_DEST_DOC =
          "A list of destination kafka topics for the sink connector, separated by commas";
  public static final String TOPICS_DEST_DEFAULT = "";
  private static final String TOPICS_DEST_DISPLAY = "The Kafka topics to map";

  public static final String MODE_CONFIG = "mode";
  private static final String MODE_DOC =
          "Behavior to handle the errors consumed." +
                  "Resend: Resend the erratic record for reprocessing to the original topic." +
                  "Remap: Use topics.destinations to map where to send the erratic records.";
  public static final String MODE_DEFAULT = "resend";
  private static final String MODE_DISPLAY = "Mode to run this connector on";

  public static final String NO_RETRIES_TOPIC_CONFIG = "final.topic.no.retries";
  private static final String NO_RETRIES_TOPIC_DOC =
          "Topic to send error messages to when there are no retries left";
  public static final String NO_RETRIES_TOPIC_DEFAULT = "error_converter_exhausted";
  private static final String NO_RETRIES_TOPIC_DISPLAY = "Final topic when no retries are left";

  public static final String CUSTOM_TOPIC_HEADER_CONFIG = "custom.topic.header.field.name";
  private static final String CUSTOM_TOPIC_HEADER_DOC =
          "Custom header key value to extract the topic from";
  public static final String CUSTOM_TOPIC_HEADER_DEFAULT = "";
  private static final String CUSTOM_TOPIC_HEADER_DISPLAY = "Custom Header Key Value for Topic";

  public static final String CUSTOM_PARTITION_HEADER_CONFIG = "custom.partition.header.field.name";
  private static final String CUSTOM_PARTITION_HEADER_DOC =
          "Custom header key value to extract the partition from";
  public static final String CUSTOM_PARTITION_HEADER_DEFAULT = "";
  private static final String CUSTOM_PARTITION_HEADER_DISPLAY = "Custom Header Key Value for Partition";

  public static final String CUSTOM_OFFSET_HEADER_CONFIG = "custom.offset.header.field.name";
  private static final String CUSTOM_OFFSET_HEADER_DOC =
          "Custom header key value to extract the offset from";
  public static final String CUSTOM_OFFSET_HEADER_DEFAULT = "";
  private static final String CUSTOM_OFFSET_HEADER_DISPLAY = "Custom Header Key Value for Offset";

  public static final String RELAYER_PREFIX = "relayer.";

  public ErrorHandlerSinkConnectorConfig(Map<?, ?> originals) {
    super(config(), originals, false);

    String mode = getString(MODE_CONFIG);
    Optional<List<String>> topics =
            getList(TOPICS_CONFIG).isEmpty()
                    ? Optional.empty()
                    : Optional.of(getList(TOPICS_CONFIG));
    Optional<List<String>> destTopics =
            getList(TOPICS_DEST_CONFIG).isEmpty()
                    ? Optional.empty()
                    : Optional.of(getList(TOPICS_DEST_CONFIG));

    if (mode.equalsIgnoreCase("resend")) {
      if (!topics.isPresent()) {
        throw new ConfigException("Cannot use resend mode without a source topic list");
      }
    }

    if (mode.equalsIgnoreCase("remap")) {
      if (!destTopics.isPresent() || !topics.isPresent()) {
        throw new ConfigException("Cannot use remap mode without topics and topics.destinations settings defined.");
      }
      if (topics.get().size() != destTopics.get().size()) {
        throw new ConfigException("Cannot use remap mode without having an equal amount of topic destinations as topics" +
                "to consume from");
      }
    }

  }

  public Map<String, Object> clientsConfig () {
    return originalsWithPrefix(RELAYER_PREFIX);
  }

  public static ConfigDef config() {

    String group = "Behavior";
    int behaviorOrder = 0;

    return new ConfigDef()
        .define(
                TOPICS_CONFIG,
                Type.LIST,
                TOPICS_DEFAULT,
                Importance.HIGH,
                TOPICS_DOC,
                group,
                ++behaviorOrder,
                ConfigDef.Width.MEDIUM,
                TOPICS_DISPLAY
        ).define(
                TOPICS_DEST_CONFIG,
                Type.LIST,
                TOPICS_DEST_DEFAULT,
                Importance.HIGH,
                TOPICS_DEST_DOC,
                group,
                ++behaviorOrder,
                ConfigDef.Width.MEDIUM,
                TOPICS_DEST_DISPLAY
        ).define(
                MODE_CONFIG,
                Type.STRING,
                MODE_DEFAULT,
                Importance.MEDIUM,
                MODE_DOC,
                group,
                ++behaviorOrder,
                ConfigDef.Width.SHORT,
                MODE_DISPLAY
        ).define(
                NO_RETRIES_TOPIC_CONFIG,
                Type.STRING,
                NO_RETRIES_TOPIC_DEFAULT,
                Importance.MEDIUM,
                NO_RETRIES_TOPIC_DOC,
                group,
                ++behaviorOrder,
                ConfigDef.Width.SHORT,
                NO_RETRIES_TOPIC_DISPLAY
        ).define(
                CUSTOM_TOPIC_HEADER_CONFIG,
                Type.STRING,
                CUSTOM_TOPIC_HEADER_DEFAULT,
                Importance.LOW,
                CUSTOM_TOPIC_HEADER_DOC,
                group,
                ++behaviorOrder,
                ConfigDef.Width.SHORT,
                CUSTOM_TOPIC_HEADER_DISPLAY
        ).define(
                CUSTOM_PARTITION_HEADER_CONFIG,
                Type.STRING,
                CUSTOM_PARTITION_HEADER_DEFAULT,
                Importance.LOW,
                CUSTOM_PARTITION_HEADER_DOC,
                group,
                ++behaviorOrder,
                ConfigDef.Width.SHORT,
                CUSTOM_PARTITION_HEADER_DISPLAY
        ).define(
                CUSTOM_OFFSET_HEADER_CONFIG,
                Type.STRING,
                CUSTOM_OFFSET_HEADER_DEFAULT,
                Importance.LOW,
                CUSTOM_OFFSET_HEADER_DOC,
                group,
                ++behaviorOrder,
                ConfigDef.Width.SHORT,
                CUSTOM_OFFSET_HEADER_DISPLAY
        );
  }
}
