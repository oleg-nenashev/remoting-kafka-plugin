package io.jenkins.plugins.remotingkafka;

import hudson.remoting.*;

import java.io.OutputStream;
import java.util.concurrent.ExecutorService;

public class KafkaChannelBuilder extends ChannelBuilder {
    public KafkaChannelBuilder(String name, ExecutorService executors) {
        super(name, executors);
    }

//    public Channel build(CommandTransport commandTransport) throws IOException {
//        return new KafkaChannel(this, commandTransport);
//    }

    @Override
    public KafkaChannelBuilder withBaseLoader(ClassLoader base) {
        return (KafkaChannelBuilder) super.withBaseLoader(base);
    }

    @Override
    public KafkaChannelBuilder withMode(Channel.Mode mode) {
        return (KafkaChannelBuilder) super.withMode(mode);
    }

    @Override
    public KafkaChannelBuilder withCapability(Capability capability) {
        return (KafkaChannelBuilder) super.withCapability(capability);
    }

    @Override
    public KafkaChannelBuilder withHeaderStream(OutputStream header) {
        return (KafkaChannelBuilder) super.withHeaderStream(header);
    }

    @Override
    public KafkaChannelBuilder withJarCache(JarCache jarCache) {
        return (KafkaChannelBuilder) super.withJarCache(jarCache);
    }

    @Override
    public KafkaChannelBuilder withoutJarCache() {
        return (KafkaChannelBuilder) super.withoutJarCache();
    }

    @Override
    public KafkaChannelBuilder withClassFilter(ClassFilter filter) {
        return (KafkaChannelBuilder) super.withClassFilter(filter);
    }
}
