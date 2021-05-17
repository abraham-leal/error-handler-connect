package com.aleal.kafka.connect;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ErrorHandlerSinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(ErrorHandlerSinkTask.class);

  protected Map<String,String> srcDestTopicMapping = new HashMap<>();
  protected KafkaProducer<Bytes, Bytes> replayProducer;
  protected KafkaConsumer<Bytes,Bytes> seekingConsumer;
  protected ErrorHandlerSinkConnectorConfig config;
  protected String noRetriesTopic;
  protected Mode mode;
  protected String custom_topic_header;
  protected String custom_partition_header;
  protected String custom_offset_header;
  protected int maxRetries;
  protected long maxSeekingBlock;
  protected enum Mode {RESEND, REMAP}

  @Override
  public void start(Map<String, String> settings) {
    this.config = new ErrorHandlerSinkConnectorConfig(settings);

    defineClients();
    setRuntimeConfigs();
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    List<ProducerRecord<Bytes,Bytes>> reroutedRecords = new ArrayList<>();

    for (SinkRecord error : records) {
      Headers thisErrorHeaders = error.headers();

      String origMessageTopic = thisErrorHeaders.lastWithName(custom_topic_header) != null ? thisErrorHeaders.lastWithName(custom_topic_header).value().toString()
              : thisErrorHeaders.lastWithName("input_record_topic") != null ? thisErrorHeaders.lastWithName("input_record_topic").value().toString()
              : thisErrorHeaders.lastWithName("__connect.errors.topic") != null ? thisErrorHeaders.lastWithName("__connect.errors.topic").value().toString()
              : "";

      int origMessagePartition = thisErrorHeaders.lastWithName(custom_partition_header) != null ? Integer.parseInt(thisErrorHeaders.lastWithName(custom_partition_header).value().toString())
              : thisErrorHeaders.lastWithName("input_record_partition") != null ? Integer.parseInt(thisErrorHeaders.lastWithName("input_record_partition").value().toString())
              : thisErrorHeaders.lastWithName("__connect.errors.partition") != null ? Integer.parseInt(thisErrorHeaders.lastWithName("__connect.errors.partition").value().toString())
              : -1;

      long origMessageOffset = thisErrorHeaders.lastWithName(custom_offset_header) != null ? Long.parseLong(thisErrorHeaders.lastWithName(custom_offset_header).value().toString())
              : thisErrorHeaders.lastWithName("input_record_offset") != null ? Long.parseLong(thisErrorHeaders.lastWithName("input_record_offset").value().toString())
              : thisErrorHeaders.lastWithName("__connect.errors.offset") != null ? Long.parseLong(thisErrorHeaders.lastWithName("__connect.errors.offset").value().toString())
              : -1;

      int currentRetries = thisErrorHeaders.lastWithName("relay_retries") != null ? Integer.parseInt(thisErrorHeaders.lastWithName("relay_retries").value().toString())
              : -1;

      if (!origMessageTopic.isEmpty() && origMessagePartition != -1 && origMessageOffset != -1) {
        if (log.isDebugEnabled()) {
          log.debug("Processing error record from: {}", origMessageTopic);
        }

        ProducerRecord<Bytes,Bytes> thisRecord = routeRecord(fetchRecord(origMessageTopic, origMessagePartition, origMessageOffset), error.topic(), currentRetries);
        if (thisRecord != null) {
          reroutedRecords.add(thisRecord);
        }
      } else {
        log.info("Skipping record: Record from topic {}, partition {}, and offset {} does not have the necessary information in headers to resend.",
                error.topic(), error.kafkaPartition(), error.kafkaOffset());
        if (log.isDebugEnabled()) {
          log.debug("Record headers: {}", error.headers());
        }
      }
    }
    sendRecordAndBlock(reroutedRecords);
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
    if (replayProducer != null) {
      replayProducer.flush();
    }
  }

  @Override
  public void stop() {
    if (replayProducer != null) {
      replayProducer.close();
    }
    if (seekingConsumer != null) {
      seekingConsumer.close();
    }
  }

  @Override
  public String version() {
    return "1.0-SNAPSHOT";
  }


  private ConsumerRecord<Bytes,Bytes> fetchRecord (String origTopic, int origPart, long origOffset) {
    // Seek to the offset of the message
    TopicPartition thisMessageTopicPartition = new TopicPartition(origTopic, origPart);
    seekingConsumer.assign(Collections.singleton(thisMessageTopicPartition));
    seekingConsumer.seek(thisMessageTopicPartition,origOffset);

    // Consume the single message
    ConsumerRecord<Bytes, Bytes> singleRecord = seekingConsumer.poll(Duration.ofMillis(maxSeekingBlock))
            .records(thisMessageTopicPartition).get(0);

    // If the offset does not exist anymore, do not process the message
    if (singleRecord.offset() != origOffset || singleRecord.partition() != origPart || !singleRecord.topic().equals(origTopic)) {
      if (log.isDebugEnabled()) {
        log.debug("Attempted to process record with offset {} and partition {} for topic {}, but it did not exist anymore as earliest offset in the partition is {}",
                origOffset, origPart, origTopic, singleRecord.offset());
      }
      return null;
    }
    seekingConsumer.unsubscribe();

    return singleRecord;
  }

  private ProducerRecord<Bytes,Bytes> routeRecord (ConsumerRecord<Bytes,Bytes> fetchedRecord, String origErrorTopic, int retries) {
    if (fetchedRecord == null) {
      return null;
    }

    ProducerRecord<Bytes, Bytes> tmpRecord;
    
    if (mode == Mode.REMAP) {
      tmpRecord = new ProducerRecord<>(srcDestTopicMapping.get(origErrorTopic),
              fetchedRecord.key(), fetchedRecord.value());
    } else {
      tmpRecord = new ProducerRecord<>(fetchedRecord.topic(),
              fetchedRecord.partition(), fetchedRecord.key(), fetchedRecord.value());
    }

    // Include a header for tracking retries
    if (maxRetries != 0) {
      ErrorHandlerSinkUtils.updateRetries(tmpRecord.headers(), retries);
    }

    // Record goes to final destination.
    if (retries > 5 || maxRetries == 0) {
      tmpRecord = normalizeFinalDestinationRecord(fetchedRecord);
    }

    return tmpRecord;
  }

  private void sendRecordAndBlock(List<ProducerRecord<Bytes,Bytes>> recordsToSend) {
    List<Future<RecordMetadata>> deliveryPromises = new ArrayList<>();

    // Send all records
    for (ProducerRecord<Bytes,Bytes> singleRecord : recordsToSend) {
      deliveryPromises.add(replayProducer.send(singleRecord));
    }

    // Await for records to be delivered. Throw an exception if delivery isn't possible.
    for (Future<RecordMetadata> promise : deliveryPromises) {
      try {
        promise.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new ConnectException("Error while sending message", e);
      }
    }
  }

  private ProducerRecord<Bytes, Bytes> normalizeFinalDestinationRecord (ConsumerRecord<Bytes,Bytes> fetchedRecord) {

    return new ProducerRecord<>(noRetriesTopic,
            fetchedRecord.partition(),fetchedRecord.key(), fetchedRecord.value());
  }


  private void defineClients() {
    Map<String, Object> srcClusterConfig = this.config.clientsConfig();

    srcClusterConfig.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "2");
    srcClusterConfig.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"1");
    srcClusterConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    srcClusterConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArraySerializer.class);
    srcClusterConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArraySerializer.class);
    srcClusterConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArrayDeserializer.class);
    srcClusterConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArrayDeserializer.class);

    this.seekingConsumer = new KafkaConsumer<>(srcClusterConfig);

    // Provide producer overrides
    srcClusterConfig.putAll(this.config.producerOverrides());
    this.replayProducer = new KafkaProducer<>(srcClusterConfig);
  }

  private void setRuntimeConfigs() {
    // Set Mode
    if (config.getString(ErrorHandlerSinkConnectorConfig.MODE_CONFIG).equalsIgnoreCase("resend")) {
      this.mode = Mode.RESEND;
      log.info("Setting mode to: resend");
    } else {
      this.mode = Mode.REMAP;
      log.info("Setting mode to: remap");
    }

    // Set topic lists
    if (mode == Mode.REMAP) {
      List<String> topicList = config.getList(ErrorHandlerSinkConnectorConfig.TOPICS_CONFIG);
      List<String> destTopicList = config.getList(ErrorHandlerSinkConnectorConfig.TOPICS_DEST_CONFIG);

      for (int i=0; i <= topicList.size()-1; i++) {
        this.srcDestTopicMapping.put(topicList.get(i), destTopicList.get(i));
      }
    }

    this.noRetriesTopic = config.getString(ErrorHandlerSinkConnectorConfig.NO_RETRIES_TOPIC_CONFIG);

    // Set custom header values
    this.custom_topic_header = config.getString(ErrorHandlerSinkConnectorConfig.CUSTOM_TOPIC_HEADER_CONFIG);
    this.custom_partition_header = config.getString(ErrorHandlerSinkConnectorConfig.CUSTOM_PARTITION_HEADER_CONFIG);
    this.custom_offset_header = config.getString(ErrorHandlerSinkConnectorConfig.CUSTOM_OFFSET_HEADER_CONFIG);

    // Set max retries
    this.maxRetries = config.getInt(ErrorHandlerSinkConnectorConfig.ERROR_HANDLER_RETRIES_CONFIG);

    // Max amount of time to block
    this.maxSeekingBlock = config.getLong(ErrorHandlerSinkConnectorConfig.MAX_SEEKING_BLOCK_MS_CONFIG);
  }
}
