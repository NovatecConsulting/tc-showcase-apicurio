import common.EnvRabbitMQConfig;
import io.apicurio.datamodels.core.models.Document;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

public class ConsumerApplication {
    private static final Logger log = Logger.getLogger(ConsumerApplication.class.getName());
    private final static String REGISTRY_URL = "http://localhost:8080/apis/registry/v2";
    private final static String API_RESOURCE = "/asyncapi.json";
    private final static String CHANNEL_NAME = "user/signedup";
    private final static UserEncoder userEncoder = new UserEncoder();

    public static void main(String[] args) throws IOException, TimeoutException {
        //get registered avro schema that is referenced in the API spec
        ApiRegistry apiRegistry = new ApiRegistry(REGISTRY_URL);
        Document apiDefinition = apiRegistry.getApiDefinition(API_RESOURCE);
        Schema avroSchema = apiRegistry.getAvroMessageSchema(apiDefinition, CHANNEL_NAME);

        //create a user and send it to the RabbitMQ broker
        Consumer consumer = new Consumer(new EnvRabbitMQConfig(), ConsumerApplication::doWork);
        consumer.consumeMessages();

        System.out.println("ConsumerApplication finished.");
    }

    /**
     * Simulates the workload of message processing and can be replaced by any other kind of method.
     * Takes a String as input and pauses the thread for the number of dots contained in the String.
     * @param receivedMessage processed message
     */
    private static void doWork(byte[] receivedMessage) {
        try {
            GenericRecord decodedRecord = userEncoder.decodeAvroUser(receivedMessage);
            System.out.println(decodedRecord);
        }catch(IOException e) {
            log.severe("Could not decode record.");
        }
    }
}

