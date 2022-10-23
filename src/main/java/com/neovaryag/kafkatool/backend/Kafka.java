package com.neovaryag.kafkatool.backend;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.Future;

import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.*;

@Slf4j
@Getter
@Component
public class Kafka {

    @Value("${max.poll.records.config}")
    private int maxPollRecords;

    @Value("${autocommit}")
    private boolean autocommit;

    @Value("${ssl.truststore.location}")
    private String sslTrustStoreLocation;

    @Value("${ssl.keystore.password}")
    private String sslTrustStorePassword;

    @Value("${ssl.truststore.type}")
    private String sslTrustStoreType;

    @Value("${ssl.keystore.location}")
    private String sslKeyStoreLocation;

    @Value("${ssl.keystore.password}")
    private String sslKeyStorePassword;

    @Value("${ssl.keystore.type}")
    private String sslKeyStoreType;

    @Value("${ssl.protocol}")
    private String sslProtocol;

    private final TreeSet<String> topicsList = new TreeSet<>();

    public Future<RecordMetadata> kafkaSend(String server, String topic, String key, String message, boolean sslEnabled) {
        //settings
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        getSslProperties(sslEnabled, props);
        //producer
        var producer = new KafkaProducer<>(props);

        //send message
        var response = producer.send(new ProducerRecord<>(topic, Long.valueOf(key), message), (metadata, exception) -> {
            if (exception != null) { //проверяю что запрос успешно отправился в очередь
                log.error(exception.getMessage());
            } else {
                log.info("message has been successfully sent");
            }
        });
        //end
        producer.close();
        return response;
    }

    private Consumer<Long, String> createConsumer(String server, String groupId, String topic, boolean sslEnabled) {
        final var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, isAutocommit());
        getSslProperties(sslEnabled, props);

        KafkaConsumer<Long, String> consumer = new KafkaConsumer<>(props);
        var rebalancedListener = new TestConsumerRebalancedListener();
        consumer.subscribe(Collections.singletonList(topic), rebalancedListener);

        return consumer;
    }

    public Map<Long, String> runConsumer(String server, String groupId, String topic, boolean sslEnabled) {
        final var consumer = createConsumer(server, groupId, topic, sslEnabled);
        var map = new HashMap<Long, String>();
        final int giveUp = 50;
        int noRecordsCount = 0;
        while (true) {
            final ConsumerRecords<Long, String> consumerRecords =
                    consumer.poll(Duration.ofMillis(10));

            if (consumerRecords.count() == 0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }

            consumerRecords.forEach(record -> {
                log.info("Consumer Record:({}, {}, {}, {})\n", record.key(), record.value(), record.partition(), record.offset());

                map.put(record.key(), record.value());
            });

            consumer.commitAsync();
        }
        consumer.close();
        return map;
    }

    public TreeSet<String> getTopics(String server, boolean sslEnabled) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, isAutocommit());
        getSslProperties(sslEnabled, props);
        Map<String, List<PartitionInfo>> topics;
        var consumer = new KafkaConsumer<>(props);
        topics = consumer.listTopics();
        consumer.close();

        topicsList.clear();
        for (Map.Entry<String, List<PartitionInfo>> entry : topics.entrySet()) {
            for (PartitionInfo partitionInfo : entry.getValue()) {
                topicsList.add(partitionInfo.topic());
            }
        }
        return topicsList;
    }


    private void getSslProperties(boolean sslEnabled, Properties props) {
        if (sslEnabled) {
            log.info("---ssl enabled---");
            props.put(SSL_TRUSTSTORE_LOCATION_CONFIG, sslTrustStoreLocation);
            props.put(SSL_TRUSTSTORE_PASSWORD_CONFIG, sslTrustStorePassword);
            props.put(SSL_TRUSTSTORE_TYPE_CONFIG, sslTrustStoreType);
            props.put(SSL_KEYSTORE_PASSWORD_CONFIG, sslKeyStorePassword);
            props.put(SSL_KEYSTORE_LOCATION_CONFIG, sslKeyStoreLocation);
            props.put(SSL_KEYSTORE_TYPE_CONFIG, sslKeyStoreType);
            props.put(SECURITY_PROTOCOL_CONFIG, sslProtocol);
            props.put(SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
        } else {
            log.info("---ssl disabled---");
            props.remove(SSL_TRUSTSTORE_LOCATION_CONFIG);
            props.remove(SSL_TRUSTSTORE_PASSWORD_CONFIG);
            props.remove(SSL_TRUSTSTORE_TYPE_CONFIG);
            props.remove(SSL_KEYSTORE_PASSWORD_CONFIG);
            props.remove(SSL_KEYSTORE_LOCATION_CONFIG);
            props.remove(SSL_KEYSTORE_TYPE_CONFIG);
            props.remove(SECURITY_PROTOCOL_CONFIG);
            props.remove(SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
        }
    }

    private static class TestConsumerRebalancedListener implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            System.out.println("Called onPartitionsRevoked with partitions:" + partitions);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            System.out.println("Called onPartitionsAssigned with partitions:" + partitions);
        }
    }

    @Bean
    public Kafka genKafka() {
        return new Kafka();
    }
}
