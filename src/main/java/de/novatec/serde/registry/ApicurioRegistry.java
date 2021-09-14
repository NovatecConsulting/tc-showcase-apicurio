package de.novatec.serde.registry;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.novatec.serde.RabbitProducerApplication;
import io.apicurio.datamodels.Library;
import io.apicurio.datamodels.asyncapi.models.AaiChannelItem;
import io.apicurio.datamodels.asyncapi.models.AaiDocument;
import io.apicurio.datamodels.core.models.Document;
import io.apicurio.registry.rest.client.RegistryClient;
import io.apicurio.registry.rest.client.RegistryClientFactory;
import io.apicurio.registry.rest.client.exception.ArtifactNotFoundException;
import io.apicurio.registry.rest.v2.beans.IfExists;
import io.apicurio.registry.types.ArtifactType;
import org.apache.avro.Schema;
import de.novatec.serde.resolver.AbsoluteAvroReference;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.logging.Logger;

public class ApicurioRegistry {
    private static final Logger log = Logger.getLogger(ApicurioRegistry.class.getName());
    private final String REGISTRY_URL;

    public ApicurioRegistry(String registryUrl) {
        REGISTRY_URL = registryUrl;
    }

    /**
     * Registers an artifact (e.g., AsyncAPI spec, avro schema) at Apicurio.
     * @param artifactID unique ID for identification
     * @param resourceAddress where resource is located locally
     * @param type of which type the artifact is, e.g., AsyncAPI, OpenAPI etc.
     */
    public void registerArtifact(String artifactID, String resourceAddress, ArtifactType type) {
        try {
            InputStream input = RabbitProducerApplication.class.getResourceAsStream(resourceAddress);
            if(input == null) {
                throw new IOException();
            }
            RegistryClient client = RegistryClientFactory.create(REGISTRY_URL);
            client.createArtifact("default", artifactID, type, IfExists.UPDATE, input);
        }catch(IOException e) {
            log.warning("Could not register artifact. Resource address is invalid.");
        }
    }

    /**
     * Gets the API definition with the specified artifactID from the registry and returns it
     * as Document object (which corresponds to an AsyncAPI object).
     * @param artifactID ID of the requested artifact
     * @return artifact transformed into a Document object
     */
    public Document getApiDefinition(String artifactID) throws NoSuchElementException {
        RegistryClient client = RegistryClientFactory.create(REGISTRY_URL);

        try {
            InputStream inputStream = client.getLatestArtifact("default", artifactID);
            return Library.readDocument(new ObjectMapper().readTree(inputStream));
        }catch (IOException | ArtifactNotFoundException e) {
            throw new NoSuchElementException("Could not get or read artifact. Check artifactID and file formatting.");
        }
    }

    public Schema getAvroMessageSchema(Document document, String channelName) throws NoSuchElementException {
        String reference = getPayloadReferenceString(document, channelName);
        return new AbsoluteAvroReference().resolveRef(reference);
    }

    private String getPayloadReferenceString(Document document, String channelName) throws NoSuchElementException {
        //walk document to get the referenced uri for the payload
        AaiDocument asyncDocument = (AaiDocument) document;
        AaiChannelItem item = asyncDocument.channels.get(channelName);
        if(item == null) {
            throw new NoSuchElementException("Channel name does not exist on provided document.");
        }
        Object payloadReference = item.subscribe.message.payload;
        if(payloadReference == null) {
            throw new NoSuchElementException("Payload object in message object does not exist on provided document.");
        }

        //transform json string to valid uri
        return payloadReference.toString().split(":\"")[1].split("\"}")[0];
    }
}
