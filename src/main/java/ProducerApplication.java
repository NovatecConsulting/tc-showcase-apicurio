import com.acme.avro.User;
import common.EnvRabbitMQConfig;
import io.apicurio.datamodels.core.models.Document;
import io.apicurio.registry.types.ArtifactType;
import org.apache.avro.Schema;
import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

public class ProducerApplication {
    private static final Logger log = Logger.getLogger(ProducerApplication.class.getName());
    private final static String REGISTRY_URL = "http://localhost:8080/apis/registry/v2";
    private final static String ARTIFACT_API = "user-registration";
    private final static String API_RESOURCE = "/asyncapi.json";
    private final static String ARTIFACT_AVRO = "user-schema";
    private final static String AVRO_RESOURCE = "/usersignedup.avsc";
    private final static String CHANNEL_NAME = "user/signedup";
    private final static UserEncoder userEncoder = new UserEncoder();
    private static Schema avroSchema;

    public static void main(String[] args) throws IOException, TimeoutException {
        //first register API and avro Schema if not present
        ApiRegistry apiRegistry = new ApiRegistry(REGISTRY_URL);
        apiRegistry.registerArtifact(ARTIFACT_API, API_RESOURCE, ArtifactType.ASYNCAPI);
        apiRegistry.registerArtifact(ARTIFACT_AVRO, AVRO_RESOURCE, ArtifactType.AVRO);

        //read message schema that is defined in the API
        Document apiDefinition = apiRegistry.getApiDefinition(API_RESOURCE);
        avroSchema = apiRegistry.getAvroMessageSchema(apiDefinition, CHANNEL_NAME);

        //create a user and send it to the RabbitMQ broker
        Producer producer = new Producer(new EnvRabbitMQConfig());
        producer.sendByteMessage(createUser("Max", "Mustermann", "max.muster@mail.com"));

        System.out.println("ProducerApplication finished.");
    }

    private static byte[] createUser(String firstName, String lastName, String email) throws IOException {
        User user = new User(
                firstName,
                lastName,
                email,
                String.valueOf(new Date().getTime())
        );
        return userEncoder.encodeAvroUser(user, avroSchema);
    }
}
