package serde;

import com.acme.avro.User;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import java.util.HashMap;
import java.util.Map;

public class SchemaRegistry {
    private final Map<String, String> configs;

    public SchemaRegistry(String registryURL) {
        this.configs = new HashMap<>();
        configs.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryURL);
    }

    public void registerAtSchemaRegistry(String topicName) {
        SpecificAvroSerializer<com.acme.avro.v2.User2> serializer = new SpecificAvroSerializer<>();
        serializer.configure(configs, false);
        serializer.serialize(topicName, com.acme.avro.v2.User2.newBuilder()
                .setFirstName("Max")
                .setLastName("Mustermann")
                .setEmail("mustermann@mail.com")
                .setCreatedAt("01.01.2021")
                .build());
        serializer.close();
    }

    public byte[] serializeUser(String topicName, User record) {
        SpecificAvroSerializer<User> serializer = new SpecificAvroSerializer<>();
        serializer.configure(configs, false);
        byte[] serialized = serializer.serialize(topicName, record);
        serializer.close();
        return serialized;
    }

    public User deserializeUser(String topicName, byte[] record) {
        SpecificAvroDeserializer<User> deserializer = new SpecificAvroDeserializer<>();
        deserializer.configure(configs, false);
        User deserialized = deserializer.deserialize(topicName, record);
        deserializer.close();
        return deserialized;
    }
}
