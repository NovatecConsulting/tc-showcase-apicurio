package de.novatec.serde.registry;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.apicurio.datamodels.Library;
import io.apicurio.datamodels.core.models.Document;
import io.apicurio.registry.rest.client.RegistryClient;
import io.apicurio.registry.rest.client.RegistryClientFactory;
import io.apicurio.registry.rest.client.exception.ArtifactNotFoundException;
import io.apicurio.registry.rest.v2.beans.IfExists;
import io.apicurio.registry.types.ArtifactType;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Logger;

@Testcontainers
public class ApicurioRegistryTest {
    private static final Logger log = Logger.getLogger(ApicurioRegistryTest.class.getName());
    private ApicurioRegistry registry;
    private String APICURIO_REGISTRY_URL;

    //resources
    private final static String API_RESOURCE = "/asyncapi.json";
    private final static String WRONG_API_RESOURCE = "/wrong-location.json";
    private final static String NO_PAYLOAD_RESOURCE = "/no-payload-asyncapi.json";
    private final static String WRONG_CHANNEL_RESOURCE = "/no-channel-asyncapi.json";
    private final static String AVRO_RESOURCE = "/usersignedup.avsc";

    //IDs and channel names
    private final static String ARTIFACT_API = "user-registration";
    private final static String WRONG_ARTIFACT_API = "wrong-id";
    private final static String CHANNEL_NAME = "user/signedup";


    @Container
    public GenericContainer<?> apicurio = new GenericContainer<>(DockerImageName.parse("apicurio/apicurio-registry-mem:2.0.1.Final"))
            .withExposedPorts(8080);

    @BeforeEach()
    public void setMappedPort() {
        int port = apicurio.getMappedPort(8080);
        String host = apicurio.getHost();
        this.APICURIO_REGISTRY_URL = "http://" + host + ":" + port + "/apis/registry/v2";

        this.registry = new ApicurioRegistry(APICURIO_REGISTRY_URL);
    }

    @Test
    public void shouldRegisterArtifact() throws IOException {
        InputStream newArtifact = ApicurioRegistryTest.class.getResourceAsStream(API_RESOURCE);
        assert newArtifact != null;
        registry.registerArtifact(ARTIFACT_API, API_RESOURCE, ArtifactType.ASYNCAPI);

        RegistryClient client = RegistryClientFactory.create(APICURIO_REGISTRY_URL);
        InputStream registeredArtifact = client.getLatestArtifact("default", ARTIFACT_API);

        Assertions.assertArrayEquals(newArtifact.readAllBytes(), registeredArtifact.readAllBytes());
    }

    @Test
    public void shouldNotRegisterArtifact() {
        registry.registerArtifact(ARTIFACT_API, WRONG_API_RESOURCE, ArtifactType.ASYNCAPI);
        RegistryClient client = RegistryClientFactory.create(APICURIO_REGISTRY_URL);

        Assertions.assertThrows(ArtifactNotFoundException.class, () ->
                client.getLatestArtifact("default", ARTIFACT_API));
    }

    @Test
    public void shouldReturnRegisteredArtifactAsDocument() throws IOException, NoSuchElementException {
        registerArtifact(ARTIFACT_API, API_RESOURCE, ArtifactType.ASYNCAPI);

        InputStream registerStream = ApicurioRegistryTest.class.getResourceAsStream(API_RESOURCE);
        Document expectedDocument = Library.readDocument(new ObjectMapper().readTree(registerStream));
        String expectedString = Library.writeDocumentToJSONString(expectedDocument);

        Document returnedDocument = registry.getApiDefinition(ARTIFACT_API);
        String returnedString = Library.writeDocumentToJSONString(returnedDocument);

        Assertions.assertNotNull(returnedDocument);
        Assertions.assertEquals(expectedString, returnedString); //convert to strings for comparison
    }

    @Test
    public void shouldThrowExceptionForGetDefinition() {
        Assertions.assertThrows(NoSuchElementException.class, () -> registry.getApiDefinition(WRONG_ARTIFACT_API));
    }

    @Test
    public void shouldThrowNoSuchElementExceptionWithoutPayload() throws IOException {
        Document noPayloadAsyncApi = resourceToDocument(NO_PAYLOAD_RESOURCE);

        Assertions.assertThrows(NoSuchElementException.class, () ->
                registry.getAvroMessageSchema(noPayloadAsyncApi, CHANNEL_NAME));
    }

    @Test
    public void shouldThrowNoSuchElementExceptionWithWrongChannel() throws IOException {
        Document wrongChannelAsyncApi = resourceToDocument(WRONG_CHANNEL_RESOURCE);

        Assertions.assertThrows(NoSuchElementException.class, () ->
                registry.getAvroMessageSchema(wrongChannelAsyncApi, CHANNEL_NAME));
    }

    @Test
    public void shouldReturnAvroSchema() throws IOException, NoSuchElementException {
        registerArtifact("User", AVRO_RESOURCE, ArtifactType.AVRO);
        Document asyncapi = resourceToDocument(API_RESOURCE);

        Schema returnedSchema = registry.getAvroMessageSchema(asyncapi, CHANNEL_NAME);
        InputStream input = ApicurioRegistryTest.class.getResourceAsStream(AVRO_RESOURCE);
        Schema expectedSchema = new Schema.Parser().parse(input);

        Assertions.assertEquals(expectedSchema, returnedSchema);
    }

    private Document resourceToDocument(String resourceAddress) throws IOException {
        InputStream registerStream = ApicurioRegistryTest.class.getResourceAsStream(resourceAddress);
        return Library.readDocument(new ObjectMapper().readTree(registerStream));
    }

    private void registerArtifact(String artifactId, String resourceAddress, ArtifactType type) {
        try {
            InputStream input = ApicurioRegistryTest.class.getResourceAsStream(resourceAddress);
            if(input == null) {
                throw new IOException();
            }
            RegistryClient client = RegistryClientFactory.create(APICURIO_REGISTRY_URL);
            client.createArtifact("default", artifactId, type, IfExists.UPDATE, input);
        }catch(IOException e) {
            log.warning("Could not register artifact. Resource address is invalid.");
        }
    }
}
