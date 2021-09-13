package de.novatec.serde;

import de.novatec.serde.rabbitmq.Consumer;
import de.novatec.serde.rabbitmq.EnvRabbitMQConfig;
import de.novatec.serde.registry.ApicurioRegistry;
import io.apicurio.datamodels.core.models.Document;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

public class RabbitConsumerApplication {
    private static final Logger log = Logger.getLogger(RabbitConsumerApplication.class.getName());
    private final static String REGISTRY_URL = "http://localhost:8080/apis/registry/v2";
    private final static String ARTIFACT_API = "user-registration";
    private final static String CHANNEL_NAME = "user/signedup";
    private final static Serde SERDE = new Serde();
    private static Schema avroSchema;

    public static void main(String[] args) throws IOException, TimeoutException {
        //get API definition from Apicurio
        ApicurioRegistry apicurioRegistry = new ApicurioRegistry(REGISTRY_URL);
        Document apiDefinition = apicurioRegistry.getApiDefinition(ARTIFACT_API);

        //get avro schema that is referenced in API definition
        avroSchema = apicurioRegistry.getAvroMessageSchema(apiDefinition, CHANNEL_NAME);

        //consume messages from RabbitMQ
        Consumer consumer = new Consumer(new EnvRabbitMQConfig(), RabbitConsumerApplication::processMessage);
        consumer.consumeMessages();

        log.info("ConsumerApplication finished.");
    }

    private static void processMessage(byte[] receivedMessage) {
        try {
            GenericRecord decodedRecord = SERDE.decodeAvroToGeneric(receivedMessage, avroSchema);
            log.info(decodedRecord.toString());
        }catch(IOException e) {
            log.warning("Could not decode record.");
        }
    }
}

