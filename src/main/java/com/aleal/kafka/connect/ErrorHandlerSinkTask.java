package com.aleal.kafka.connect;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ErrorHandlerSinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(ErrorHandlerSinkTask.class);

  protected ErrorHandlerSinkConnectorConfig config;
  protected KafkaProducer<Bytes, Bytes> replayProducer;
  protected KafkaConsumer<Bytes,Bytes> seekingConsumer;
  protected Map<String,String> srcDestTopicMapping = new HashMap<>();
  protected String noRetriesTopic;
  protected Mode mode;

  public enum Mode {
    RESEND,
    REMAP
  }

  private void defineClients() {
    Map<String, Object> srcClusterConfig = this.config.clientsConfig();

    srcClusterConfig.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"1");
    srcClusterConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    srcClusterConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArraySerializer.class);
    srcClusterConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArraySerializer.class);
    srcClusterConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArrayDeserializer.class);
    srcClusterConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArrayDeserializer.class);

    this.replayProducer = new KafkaProducer<>(srcClusterConfig);
    this.seekingConsumer = new KafkaConsumer<>(srcClusterConfig);
  }

  private void setMode() {
    if (this.config.getString(ErrorHandlerSinkConnectorConfig.MODE_CONFIG).equalsIgnoreCase("resend")) {
      this.mode = Mode.RESEND;
      log.info("Setting mode to: resend");
    } else {
      this.mode = Mode.REMAP;
      log.info("Setting mode to: remap");
    }
  }

  private void mapDestinations() {
    if (this.mode == Mode.REMAP) {
      List<String> topicList = this.config.getList(ErrorHandlerSinkConnectorConfig.TOPICS_CONFIG);
      List<String> destTopicList = this.config.getList(ErrorHandlerSinkConnectorConfig.TOPICS_DEST_CONFIG);

      for (int i=0; i <= topicList.size()-1; i++) {
        this.srcDestTopicMapping.put(topicList.get(i), destTopicList.get(i));
      }
    }

    this.noRetriesTopic = this.config.getString(ErrorHandlerSinkConnectorConfig.NO_RETRIES_TOPIC_CONFIG);
  }

  @Override
  public void start(Map<String, String> settings) {
    this.config = new ErrorHandlerSinkConnectorConfig(settings);

    defineClients();
    setMode();
    mapDestinations();
  }

  private void fetchAndSend (String origTopic, int origPart, long origOffset, int retries) {
    ProducerRecord<Bytes, Bytes> toSend;

    // Seek to the offset of the message
    TopicPartition thisMessageTopicPartition = new TopicPartition(origTopic, origPart);
    this.seekingConsumer.assign(Collections.singleton(thisMessageTopicPartition));
    this.seekingConsumer.seek(thisMessageTopicPartition,origOffset);

    // Consume the message
    ConsumerRecords<Bytes, Bytes> oneRecordBatch = seekingConsumer.poll(Duration.ofMillis(30000));
    for (ConsumerRecord<Bytes, Bytes> interestingRecord : oneRecordBatch) {
      if (this.mode == Mode.REMAP) {
        toSend = new ProducerRecord<>(this.srcDestTopicMapping.get(origTopic),
                origPart, interestingRecord.key(), interestingRecord.value());
      } else {
        toSend = new ProducerRecord<>(origTopic,
                origPart, interestingRecord.key(), interestingRecord.value());
      }

      if (retries == -1) { // relay provenance header setting
        toSend.headers().add("relay_retries", "1".getBytes(StandardCharsets.UTF_8));
      } else { // update retries
        toSend.headers().remove("relay_retries");
        toSend.headers().add("relay_retries", String.valueOf(retries + 1).getBytes(StandardCharsets.UTF_8));
      }

      if (retries > 5) {
        toSend = new ProducerRecord<>(this.noRetriesTopic,
                origPart,interestingRecord.key(), interestingRecord.value());
      }

      this.replayProducer.send(toSend);
    }
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    for (SinkRecord error : records) {
      Headers thisErrorHeaders = error.headers();
      String origMessageTopic = "";
      int origMessagePartition = -1;
      int currentRetries = -1;
      long origMessageOffset = -1;

      for (Header oneHeader : thisErrorHeaders) {
        if (oneHeader.key().equals("input_record_topic") || oneHeader.key().equals("__connect.errors.topic")) {
          origMessageTopic = oneHeader.value().toString();
        }
        if (oneHeader.key().equals("input_record_partition") || oneHeader.key().equals("__connect.errors.partition")) {
          origMessagePartition = Integer.parseInt(oneHeader.value().toString());
        }
        if (oneHeader.key().equals("input_record_offset") || oneHeader.key().equals("__connect.errors.offset")) {
          origMessageOffset = Long.parseLong(oneHeader.value().toString());
        }
        if (oneHeader.key().equals("relayer_has_retried")) {
          currentRetries = Integer.parseInt(oneHeader.value().toString());
        }
      }

      if (!origMessageTopic.isEmpty() && origMessagePartition != -1 && origMessageOffset != -1) {
        if (log.isDebugEnabled()) {
          log.debug("Processing error record from: " + origMessageTopic);
        }
        fetchAndSend(origMessageTopic, origMessagePartition, origMessageOffset, currentRetries);
      }

      this.seekingConsumer.unsubscribe();
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
    if (this.replayProducer != null) {
      this.replayProducer.flush();
    }
  }

  @Override
  public void stop() {
    if (this.replayProducer != null) {
      this.replayProducer.close();
    }
    if (this.seekingConsumer != null) {
      this.seekingConsumer.close();
    }
  }

  @Override
  public String version() {
    return "1.0-SNAPSHOT";
  }
}
