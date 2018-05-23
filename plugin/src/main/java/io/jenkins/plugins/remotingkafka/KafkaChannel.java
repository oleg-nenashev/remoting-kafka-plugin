package io.jenkins.plugins.remotingkafka;

import hudson.remoting.Channel;
import hudson.remoting.CommandTransport;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.logging.Logger;

public class KafkaChannel extends Channel {
    private static final Logger LOGGER = Logger.getLogger(KafkaChannel.class.getName());

    private final CommandTransport transport;

    protected KafkaChannel(@Nonnull KafkaChannelBuilder settings, @Nonnull CommandTransport transport)
            throws IOException {
        super(settings, transport);
        this.transport = transport;
    }
}
