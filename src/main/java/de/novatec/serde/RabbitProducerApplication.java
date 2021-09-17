package de.novatec.serde;

import com.acme.avro.User;
import de.novatec.serde.rabbitmq.EnvRabbitMQConfig;
import de.novatec.serde.rabbitmq.Producer;
import de.novatec.serde.registry.ApicurioRegistry;
import de.novatec.serde.registry.SchemaRegistry;
import io.apicurio.datamodels.asyncapi.models.AaiDocument;
import io.apicurio.registry.types.ArtifactType;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

public class RabbitProducerApplication {
    private static final Logger log = Logger.getLogger(RabbitProducerApplication.class.getName());
    private final static String APICURIO_REGISTRY_URL = "http://localhost:8080/apis/registry/v2";
    private final static String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    private final static String TOPIC_NAME = "topic";
    private final static String ARTIFACT_API = "user-registration";
    private final static String API_RESOURCE = "/asyncapi.json";
    private final static Serde SERDE = new Serde();

    public static void main(String[] args) throws IOException, TimeoutException {
        //register avro schema at Schema Registry
        SchemaRegistry schemaRegistry = new SchemaRegistry(SCHEMA_REGISTRY_URL);
        schemaRegistry.registerAtSchemaRegistry(User.getClassSchema(), TOPIC_NAME);

        //register API definition at Apicurio
        ApicurioRegistry apicurioRegistry = new ApicurioRegistry(APICURIO_REGISTRY_URL);
        apicurioRegistry.registerArtifact(ARTIFACT_API, API_RESOURCE, ArtifactType.ASYNCAPI);

        //get API definition and specified url of the broker
        AaiDocument apiDefinition = (AaiDocument) apicurioRegistry.getApiDefinition(ARTIFACT_API);

        String url = apiDefinition.servers.get("production").url;
        Map<String, String> rabbitMQConfig = new HashMap<>();
        rabbitMQConfig.put("HOST", url.split(":")[0]);
        rabbitMQConfig.put("PORT", url.split(":")[1]);

        //create a user and send it to the RabbitMQ broker
        Producer producer = new Producer(new EnvRabbitMQConfig(rabbitMQConfig));
        User user = new User("Max", "Mustermann", "max.muster@mail.com", String.valueOf(new Date().getTime()));
        producer.sendByteMessage(SERDE.encodeToAvro(user));
        producer.stop();

        log.info("ProducerApplication finished.");
    }
}


