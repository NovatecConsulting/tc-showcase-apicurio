package de.novatec.serde.registry;

import com.acme.avro.User;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class SchemaRegistry {
    private final Map<String, String> configs;

    public SchemaRegistry(String registryURL) {
        this.configs = new HashMap<>();
        configs.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryURL);
    }

    public void registerAtSchemaRegistry(String topicName) {
        SchemaRegistryClient client = new CachedSchemaRegistryClient(
                configs.get(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG), 1);

        AvroSchemaProvider provider = new AvroSchemaProvider();
        Optional<ParsedSchema> parsedSchema = provider.parseSchema(
                com.acme.avro.v2.User.getClassSchema().toString(),
                new ArrayList<>()
        );

        parsedSchema.ifPresent(schema -> {
            try {
                client.register(topicName, schema);
            } catch (IOException | RestClientException e) {
                e.printStackTrace();
            }
        });
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
