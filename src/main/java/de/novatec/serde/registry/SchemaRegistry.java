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
import org.apache.avro.Schema;

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

    /**
     * Registers an Avro schema at Confluent Schema Registry. Uses the provided topic name for identification.
     * @param topicName unique name
     */
    public void registerAtSchemaRegistry(Schema schema, String topicName) {
        SchemaRegistryClient client = new CachedSchemaRegistryClient(
                configs.get(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG), 1);

        AvroSchemaProvider provider = new AvroSchemaProvider();
        Optional<ParsedSchema> parsedSchema = provider.parseSchema(
                schema.toString(),
                new ArrayList<>()
        );

        parsedSchema.ifPresent(s -> {
            try {
                client.register(topicName, s);
            } catch (IOException | RestClientException e) {
                e.printStackTrace();
            }
        });
    }

    /**
     * Serializes a User object using Avro and returns a byte array.
     * This method is not used in the showcase but is kept for example purposes.
     * @param topicName Schema Registry topic name of the Avro schema
     * @param record User record to serialize
     * @return serialized User object
     */
    public byte[] serializeUser(String topicName, User record) {
        SpecificAvroSerializer<User> serializer = new SpecificAvroSerializer<>();
        serializer.configure(configs, false);
        byte[] serialized = serializer.serialize(topicName, record);
        serializer.close();
        return serialized;
    }

    /**
     * Deserializes a byte array using Avro to a User object.
     * This method is not used in the showcase but is kept for example purposes.
     * @param topicName Schema Registry topic name of the Avro schema
     * @param record byte array record to deserialize
     * @return deserialized User object
     */
    public User deserializeUser(String topicName, byte[] record) {
        SpecificAvroDeserializer<User> deserializer = new SpecificAvroDeserializer<>();
        deserializer.configure(configs, false);
        User deserialized = deserializer.deserialize(topicName, record);
        deserializer.close();
        return deserialized;
    }
}
