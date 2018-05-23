package io.jenkins.plugins.remotingkafka.serializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import hudson.remoting.Command;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class CommandDeserializer implements Deserializer<Command> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public Command deserialize(String s, byte[] bytes) {
        ObjectMapper mapper = new ObjectMapper();
        Command command = null;
        try {
            command = mapper.readValue(bytes, Command.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return command;
    }

    @Override
    public void close() {

    }
}
