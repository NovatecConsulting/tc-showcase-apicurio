package de.novatec.serde.registry;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.novatec.serde.RabbitProducerApplication;
import de.novatec.serde.resolver.AbsoluteAvroReference;
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

import java.io.IOException;
import java.io.InputStream;
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
     * @throws ArtifactNotFoundException when the artifact with the specified ID does not exist in Apicurio Registry
     * @throws IOException when the returned document is not in valid JSON format
     */
    public Document getApiDefinition(String artifactID) throws ArtifactNotFoundException, IOException {
        RegistryClient client = RegistryClientFactory.create(REGISTRY_URL);

        try(InputStream inputStream = client.getLatestArtifact("default", artifactID)) {
            return Library.readDocument(new ObjectMapper().readTree(inputStream));
        }catch (IOException e) {
            throw new IOException("Returned artifact is not in valid JSON format.");
        }
    }

    /**
     * Extracts the reference of an Avro schema of a message in a channel within an AsyncAPI document.
     * This reference is resolved and returned as Avro schema.
     *
     * The Avro schema can currently either be registered at Apicurio Registry or at Confluent Schema Registry.
     * @param document to get the Avro schema from
     * @param channelName where to look for the reference
     * @return referenced Avro schema
     * @throws NoSuchChannelException when the specified channel does not exist on the document
     * @throws NoPayloadException when the message in the channels does not include a payload object
     */
    public Schema getAvroMessageSchema(Document document, String channelName) throws NoSuchChannelException, NoPayloadException {
        String reference = getPayloadReferenceString(document, channelName);
        return new AbsoluteAvroReference().resolveRef(reference);
    }

    private String getPayloadReferenceString(Document document, String channelName) throws NoSuchChannelException, NoPayloadException {
        //walk document to get the referenced uri for the payload
        AaiDocument asyncDocument = (AaiDocument) document;
        AaiChannelItem item = asyncDocument.channels.get(channelName);
        if(item == null) {
            throw new NoSuchChannelException("Channel name does not exist on provided document.");
        }
        Object payloadReference = item.subscribe.message.payload;
        if(payloadReference == null) {
            throw new NoPayloadException("Payload object in message object does not exist on provided document.");
        }

        //transform json string to valid uri
        return payloadReference.toString().split(":\"")[1].split("\"}")[0];
    }
}
