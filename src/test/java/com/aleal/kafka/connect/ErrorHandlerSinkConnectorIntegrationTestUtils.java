package com.aleal.kafka.connect;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class ErrorHandlerSinkConnectorIntegrationTestUtils {
    private static Logger log = LoggerFactory.getLogger(ErrorHandlerSinkConnectorIntegrationTestUtils.class);
    KafkaProducer<String,String> testingProducer;
    KafkaConsumer<String,String> testingConsumer;
    private final String connectUrl;
    private final String kafkaBootstrap;
    private final String kafkaContainerAlias;

    public ErrorHandlerSinkConnectorIntegrationTestUtils (String internalKafkaBootstrap, String kafkaBootstrap, String connectURL) {
        this.kafkaContainerAlias = internalKafkaBootstrap;
        this.kafkaBootstrap = kafkaBootstrap;
        this.connectUrl = connectURL;
        this.testingProducer = new KafkaProducer<>(getClientConfigs());
        this.testingConsumer = new KafkaConsumer<>(getClientConfigs());
    }

    protected void waitForTopicPartitionToPopulate (String topic, int partition, int timeoutMs) throws ExecutionException, InterruptedException {
        AdminClient thisAdmin = AdminClient.create(getClientConfigs());
        TopicPartition targetPartition = new TopicPartition(topic,partition);
        Map<TopicPartition, OffsetSpec> instructions = new HashMap<>();
        instructions.put(targetPartition, OffsetSpec.latest());
        long startTime = System.currentTimeMillis();
        while (true) {
            ListOffsetsResult result = thisAdmin.listOffsets(instructions);
            ListOffsetsResult.ListOffsetsResultInfo expectedPartition = result.partitionResult(targetPartition).get();
            if (expectedPartition.offset() > 0) {
                log.info("Offset at partition: {}", expectedPartition.offset());
                break;
            }
            if (System.currentTimeMillis() > (startTime+timeoutMs)){
                log.warn("Timeout reached for partition population");
                break;
            }
        }
    }

    protected void ensureTopicsExist(List<String> topics) throws ExecutionException, InterruptedException {
        AdminClient thisAdmin = AdminClient.create(getClientConfigs());
        Collection<NewTopic> newTopicsToCreate = new ArrayList<>();
        for (String topic : topics) {
            NewTopic aTopic = new NewTopic(topic,1,(short) 1);
            newTopicsToCreate.add(aTopic);
        }
        thisAdmin.createTopics(newTopicsToCreate);
        while (true) {
            Set<String> existingTopics = thisAdmin.listTopics().names().get();
            if (existingTopics.containsAll(topics)){
                log.info("Topics created in cluster: {}", existingTopics);
                return;
            }
        }
    }

    protected RecordMetadata produce(String topic, String key, String value) throws ExecutionException, InterruptedException {
        ProducerRecord<String,String> thisMessage = new ProducerRecord<>(topic,key,value);
        return testingProducer.send(thisMessage).get();
    }

    protected RecordMetadata produceError(String errorTopic, String sourceTopic, int sourcePartition, long sourceOffset, String key, String value) throws ExecutionException, InterruptedException {
        ProducerRecord<String,String> thisMessage = new ProducerRecord<>(errorTopic,key,value);
        thisMessage.headers().add("input_record_topic", sourceTopic.getBytes(StandardCharsets.UTF_8));
        thisMessage.headers().add("input_record_partition", String.valueOf(sourcePartition).getBytes(StandardCharsets.UTF_8));
        thisMessage.headers().add("input_record_offset", String.valueOf(sourceOffset).getBytes(StandardCharsets.UTF_8));
        return testingProducer.send(thisMessage).get();
    }

    protected ConsumerRecord<String,String> consume (String topic, int partition, long offset) {
        TopicPartition thisMessageTP = new TopicPartition(topic,partition);
        testingConsumer.assign(Collections.singleton(thisMessageTP));
        testingConsumer.seek(thisMessageTP, offset);

        // Consume the single message
        List<ConsumerRecord<String, String>> batchRecords = testingConsumer.poll(Duration.ofMillis(30000))
                .records(thisMessageTP);

        log.info("Number of records in batch: {}", batchRecords.size());

        ConsumerRecord<String,String> singleRecord = batchRecords.get(0);

        testingConsumer.unsubscribe();

        return singleRecord;
    }

    protected void postErrorHandlingConnector (String mode, List<String> topics, List<String> destinationTopics) throws IOException {
        try(CloseableHttpClient httpclient = HttpClients.createDefault()) {
            HttpPost httppost = new HttpPost(String.format("http://%s/connectors", connectUrl));
            httppost.addHeader("Content-Type","application/json");

            String joinedSrcTopics = String.join(",", topics);
            String joinedDestTopics = String.join(",", destinationTopics);

            String connectorProperties = String.format("{ \"name\" : \"MyErrorHandlerConnector\", \"config\" : {\"error.handler.bootstrap.servers\":\"%s\", " +
                    "\"mode\":\"%s\", \"topics\":\"%s\", \"topics.destinations\":\"%s\", \"connector.class\": \"com.aleal.kafka.connect.ErrorHandlerSinkConnector\"}}", kafkaContainerAlias, mode, joinedSrcTopics, joinedDestTopics);
            StringEntity body = new StringEntity(connectorProperties);

            httppost.setEntity(body);

            HttpResponse response = httpclient.execute(httppost);

            if (response.getStatusLine().getStatusCode() < 400) {
                log.info("Successfully posted connector in mode: {} with source topics: {} and destination topics: {}", mode, joinedSrcTopics, joinedDestTopics);
                log.info("Status Returned: {}", response.getStatusLine().getStatusCode());
            } else {
                log.info("Failed to post connector with status code: {}", response.getStatusLine().getStatusCode());
                log.info("Payload: {}", connectorProperties);
                log.info("Response Body: {}", EntityUtils.toString(response.getEntity()));
                throw new IOException();
            }
        }
    }

    protected Map<String,Object> getClientConfigs() {
        Map<String,Object> thisConfigs = new HashMap<>();

        thisConfigs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap);
        thisConfigs.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "2");
        thisConfigs.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"1");
        thisConfigs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        thisConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        thisConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        thisConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
        thisConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);

        return thisConfigs;
    }
}
