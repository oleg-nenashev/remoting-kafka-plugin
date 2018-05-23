package io.jenkins.plugins.remotingkafka.commandtransport;

import hudson.remoting.Capability;
import hudson.remoting.Command;
import hudson.remoting.SynchronousCommandTransport;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

/**
 * Referenced from ClassicCommandTransport.
 */
public class KafkaClassicCommandTransport extends SynchronousCommandTransport {
    private static final Logger LOGGER = Logger.getLogger(KafkaClassicCommandTransport.class.getName());
    private final Capability remoteCapability;
    // We use a single instance producer/consumer for each command transport for now.
    private final Producer<String, Command> producer;
    private final Consumer<String, Command> consumer;
    private final String producerTopic;
    private final String producerKey;
    private final List<String> consumerTopics;
    private final String consumerKey;
    private final long pollTimeout;

    public KafkaClassicCommandTransport(Capability remoteCapability, String producerTopic, String producerKey
            , List<String> consumerTopics, String consumerKey, long pollTimeout
            , Producer<String, Command> producer, Consumer<String, Command> consumer) {
        this.remoteCapability = remoteCapability;
        this.producerKey = producerKey;
        this.producerTopic = producerTopic;
        this.consumerKey = consumerKey;
        this.consumerTopics = consumerTopics;
        this.producer = producer;
        this.consumer = consumer;
        this.pollTimeout = pollTimeout;
    }

    @Override
    public Capability getRemoteCapability() throws IOException {
        return remoteCapability;
    }

    @Override
    protected void write(Command cmd, boolean last) throws IOException {
        producer.send(new ProducerRecord<String, Command>(producerTopic, producerKey, cmd));
    }

    @Override
    public void closeWrite() throws IOException {
        // Because Kafka producer is thread safe, we do not need to close the producer and may reuse.
        producer.close();
    }

    @Override
    public void closeRead() throws IOException {
        consumer.commitSync();
        consumer.close();
    }

    @Override
    public Command read() throws IOException, ClassNotFoundException, InterruptedException {
        Command cmd = null;
        consumer.subscribe(consumerTopics);
        while (true) {
            ConsumerRecords<String, Command> records = consumer.poll(pollTimeout);
            for (ConsumerRecord<String, Command> record : records) {
                if (record.key().equals(consumerKey)) {
                    cmd = record.value();
                }
            }
            if (cmd != null) return cmd;
        }
    }
}
