package io.jenkins.plugins.remotingkafka;

import hudson.Extension;
import hudson.model.Computer;
import hudson.model.Descriptor;
import hudson.model.TaskListener;
import hudson.remoting.*;
import hudson.slaves.ComputerLauncher;
import hudson.slaves.SlaveComputer;
import io.jenkins.plugins.remotingkafka.commandtransport.KafkaClassicCommandTransport;
import jenkins.model.JenkinsLocationConfiguration;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.kohsuke.stapler.DataBoundConstructor;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

public class KafkaComputerLauncher extends ComputerLauncher {
    private static final Logger LOGGER = Logger.getLogger(KafkaComputerLauncher.class.getName());

    @DataBoundConstructor
    public KafkaComputerLauncher() {

    }

    @Override
    public boolean isLaunchSupported() {
        return true;
    }

    @Override
    public synchronized void launch(SlaveComputer computer, final TaskListener listener) throws IOException, InterruptedException {
        ChannelBuilder cb = new ChannelBuilder(computer.getNode().getNodeName(), Computer.threadPoolForRemoting)
                .withHeaderStream(listener.getLogger());
        CommandTransport ct = makeTransport(computer);
        computer.setChannel(cb, ct, new Channel.Listener() {
            @Override
            public void onClosed(Channel channel, IOException cause) {
                super.onClosed(channel, cause);
            }
        });
        // testing code.
//        KafkaProducerClient producer = KafkaProducerClient.getInstance();
//        KafkaConsumerClient consumer = KafkaConsumerClient.getInstance();
//        JenkinsLocationConfiguration loc = JenkinsLocationConfiguration.get();
//        String jenkinsURL = (loc != null && loc.getUrl() != null) ? loc.getUrl() : "http://localhost:8080";
//        URL url = new URL(jenkinsURL);
//        String masterAgentConnectionTopic = url.getHost() + "-" + url.getPort() + "-" + computer.getName()
//                + KafkaConstants.CONNECT_SUFFIX;
//        String agentMasterConnectionTopic = computer.getName() + "-" + url.getHost() + "-" + url.getPort()
//                + KafkaConstants.CONNECT_SUFFIX;
//        Properties producerProps = GlobalKafkaProducerConfiguration.get().getProps();
//        Properties consumerProps = GlobalKafkaConsumerConfiguration.get().getProps();
//        producer.send(producerProps, masterAgentConnectionTopic, null, agentMasterConnectionTopic);
//        consumer.subscribe(consumerProps, Arrays.asList(agentMasterConnectionTopic), 0);
    }

    private CommandTransport makeTransport(SlaveComputer computer) {
        JenkinsLocationConfiguration loc = JenkinsLocationConfiguration.get();
        String jenkinsURL = (loc != null && loc.getUrl() != null) ? loc.getUrl() : "http://localhost:8080";
        URL url;
        try {
            url = new URL(jenkinsURL);
        } catch (MalformedURLException e) {
            throw new IllegalStateException("Malformed Jenkins URL exception");
        }
        Capability cap = new Capability();
        String producerKey = "launch", consumerKey = "launch";
        String producerTopic = url.getHost() + "-" + url.getPort() + "-" + computer.getName()
                + KafkaConstants.CONNECT_SUFFIX;
        List<String> consumerTopics = Arrays.asList(computer.getName() + "-" + url.getHost() + "-" + url.getPort()
                + KafkaConstants.CONNECT_SUFFIX);

        Properties producerProps = GlobalKafkaProducerConfiguration.get().getProps();
        if (producerProps.getProperty(KafkaConstants.BOOTSTRAP_SERVERS) == null) {
            throw new IllegalStateException("Please provide Kafka producer connection URL in global setting");
        }
        producerProps.put(KafkaConstants.KEY_SERIALIZER, "io.jenkins.plugins.remotingkafka.serializers.CommandSerializer");
        producerProps.put(KafkaConstants.VALUE_SERIALIZER, "io.jenkins.plugins.remotingkafka.serializers.CommandSerializer");
        Producer<String, Command> producer = new KafkaProducer<String, Command>(producerProps);

        Properties consumerProps = GlobalKafkaConsumerConfiguration.get().getProps();
        if (consumerProps.getProperty(KafkaConstants.BOOTSTRAP_SERVERS) == null) {
            throw new IllegalStateException("Please provide Kafka consumer connection URL in global setting");
        }
        consumerProps.put(KafkaConstants.KEY_DESERIALIZER, "io.jenkins.plugins.remotingkafka.serializers.CommandDeserializer");
        consumerProps.put(KafkaConstants.VALUE_DESERIALIZER, "io.jenkins.plugins.remotingkafka.serializers.CommandDeserializer");
        KafkaConsumer<String, Command> consumer = new KafkaConsumer<String, Command>(consumerProps);
        return new KafkaClassicCommandTransport(cap, producerTopic, producerKey, consumerTopics, consumerKey, 0, producer, consumer);
    }

    @Extension
    public static class DescriptorImpl extends Descriptor<ComputerLauncher> {
        public String getDisplayName() {
            return "Launch agents with Kafka";
        }
    }
}
