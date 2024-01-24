package com.redhat;

import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;

import static java.time.Duration.ofMillis;
import static java.util.Collections.singleton;

public class Consumer implements ConsumerRebalanceListener, OffsetCommitCallback {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID = "my-group";
    private static final long POLL_TIMEOUT_MS = 1_000L;
    private static final String TOPIC_NAME = "my-topic";
//    private static final long NUM_MESSAGES = 50;
    private static final long NUM_MESSAGES = Long.MAX_VALUE;
    private static final long NUM_FAILS = 1000;
//    private static final long PROCESSING_DELAY_MS = 1_000L;
    private static final long PROCESSING_DELAY_MS = 10L;

    private KafkaConsumer<Long, byte[]> kafkaConsumer;
    protected AtomicLong messageCount = new AtomicLong(0);
    private Map<TopicPartition, OffsetAndMetadata> pendingOffsets = new HashMap<>();

    private Map<Long, Long> messagesPerTimestamp = new HashMap<>();
    private Map<Integer, Long> messagesPerPartition = new HashMap<>();

    public static void main(String[] args) {
        new Consumer().run();
    }

    private void addOrIncrementLongValue(Object key, Map map)
    {
        if (key == null)
            return;

        if (map.containsKey(key))
        {
            Long value = (Long) map.get(key);
            value++;
            map.put(key, value);
        }
        else
            map.put(key, 1L);
    }
    public void run() {
        System.out.println("Running consumer");
        try (var consumer = createKafkaConsumer()) {
            kafkaConsumer = consumer;
            consumer.subscribe(singleton(TOPIC_NAME), this);
            System.out.printf("Subscribed to %s%n", TOPIC_NAME);
            consumer.seekToBeginning(consumer.assignment());
            int noRecords=0;

            while (noRecords < NUM_FAILS)
            {
                try {
                    ConsumerRecords<Long, byte[]> records = consumer.poll(ofMillis(POLL_TIMEOUT_MS));
                    if (!records.isEmpty()) {
                        for (ConsumerRecord<Long, byte[]> record : records) {
                            Long timestamp = record.timestamp();
                            int partition = record.partition();
                            addOrIncrementLongValue(timestamp, messagesPerTimestamp);
                            addOrIncrementLongValue(partition, messagesPerPartition);
//                            Instant instant = Instant.ofEpochMilli(timestamp);
//                            System.out.printf("Record fetched from %s-%d with offset %d, date: %s%n",
//                                record.topic(), partition, record.offset(), instant);
//                            sleep(PROCESSING_DELAY_MS);

                            pendingOffsets.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1, null));
                            if (messageCount.incrementAndGet() == NUM_MESSAGES) {
                                break;
                            }
                            if (messageCount.get()%1000==0)
                                System.out.printf(".");

                        }
                        consumer.commitAsync(pendingOffsets, this);
                        pendingOffsets.clear();
                    }
                    else
                    {
                        ++noRecords;
                        if (noRecords%(NUM_FAILS/4) == 0) {
                            System.out.printf("%nFail reads increasing .. still working");
                            sleep(PROCESSING_DELAY_MS);
                        }
                    }
                } catch (OffsetOutOfRangeException | NoOffsetForPartitionException e) {
                    System.out.println("Invalid or no offset found, and auto.reset.policy unset, using latest");
                    consumer.seekToEnd(e.partitions());
                    consumer.commitSync();
                } catch (Exception e) {
                    System.err.println(e.getMessage());
                    if (!retriable(e)) {
                        e.printStackTrace();
                        System.exit(1);
                    }
                }
            }
            System.out.printf("%n%nTotal messages consumed: %d%n", messageCount.get());
            System.out.println("Messages per timestamp:");
            long sum = 0;
            for (long timestamp: messagesPerTimestamp.keySet())
            {
                long count = messagesPerTimestamp.get(timestamp);
                System.out.printf("Timestamp: %s, messages: %d%n", Instant.ofEpochMilli(timestamp), count);
                sum += count;
            }
            System.out.printf("Total messages consumed (timestamp count): %d%n", sum);

            System.out.println("Messages per partition:");
            sum = 0;
            for (int partition: messagesPerPartition.keySet())
            {
                long count = messagesPerPartition.get(partition);
                System.out.printf("Partition: %d, messages: %d%n", partition, count);
                sum += count;
            }
            System.out.printf("Total messages consumed (partition count): %d%n", sum);
        }
    }

    private KafkaConsumer<Long, byte[]> createKafkaConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "client-" + UUID.randomUUID());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(props);
    }

    private void sleep(long ms) {
        try {
            TimeUnit.MILLISECONDS.sleep(ms);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean retriable(Exception e) {
        if (e == null) {
            return false;
        } else if (e instanceof IllegalArgumentException
            || e instanceof UnsupportedOperationException
            || !(e instanceof RebalanceInProgressException)
            || !(e instanceof RetriableException)) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        System.out.printf("Assigned partitions: %s%n", partitions);
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        System.out.printf("Revoked partitions: %s%n", partitions);
        kafkaConsumer.commitSync(pendingOffsets);
        pendingOffsets.clear();
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        System.out.printf("Lost partitions: {}", partitions);
    }

    @Override
    public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
        if (e != null) {
            System.err.println("Failed to commit offsets");
            if (!retriable(e)) {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }
}