package de.novatec.serde.registry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.novatec.serde.RabbitProducerApplication;
import io.apicurio.datamodels.Library;
import io.apicurio.datamodels.asyncapi.models.AaiChannelItem;
import io.apicurio.datamodels.asyncapi.models.AaiDocument;
import io.apicurio.datamodels.core.models.Document;
import io.apicurio.datamodels.core.util.IReferenceResolver;
import io.apicurio.registry.rest.client.RegistryClient;
import io.apicurio.registry.rest.client.RegistryClientFactory;
import io.apicurio.registry.rest.v2.beans.IfExists;
import io.apicurio.registry.types.ArtifactType;
import org.apache.avro.Schema;
import de.novatec.serde.resolver.AbsoluteAvroReference;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

public class ApicurioRegistry {
    private final String REGISTRY_URL;

    public ApicurioRegistry(String registryUrl) {
        REGISTRY_URL = registryUrl;
    }

    /**
     * Registers an artifact (e.g., AsyncAPI spec, avro schema) at Apicurio.
     * @param artifactID unique ID for identification
     * @param resourceAddress where resource is located locally
     * @param type of which type the artifact is, e.g., AsyncAPI, OpenAPI etc.
     * @throws IOException
     */
    public void registerArtifact(String artifactID, String resourceAddress, ArtifactType type) throws IOException {
        try(InputStream input = Objects.requireNonNull(RabbitProducerApplication.class.getResource(resourceAddress)).openStream()) {
            RegistryClient client = RegistryClientFactory.create(REGISTRY_URL);
            client.createArtifact("default", artifactID, type, IfExists.UPDATE, input);
        }
    }

    /**
     * Gets the API definition with the specified artifactID from the registry and returns it
     * as Document object (which corresponds to an AsyncAPI object).
     * @param artifactID ID of the requested artifact
     * @return artifact transformed into a Document object
     * @throws IOException
     */
    public Document getApiDefinition(String artifactID) throws IOException {
        RegistryClient client = RegistryClientFactory.create(REGISTRY_URL);
        InputStream inputStream = client.getLatestArtifact("default", artifactID);
        JsonNode node =  new ObjectMapper().readTree(inputStream);

        return Library.readDocument(node);
    }

    public Schema getAvroMessageSchema(Document document, String channelName) {
        //walk document to get the referenced uri for the payload
        AaiDocument asyncDocument = (AaiDocument) document;
        AaiChannelItem item = asyncDocument.channels.get(channelName);
        Object payloadReference = item.subscribe.message.payload;

        //transform json string to valid uri
        String referenceUri1 = payloadReference.toString().split(":\"")[1];
        String referenceUri2 = referenceUri1.split("\"}")[0];

        //get schema from remote reference
        AbsoluteAvroReference resolver = new AbsoluteAvroReference();
        return resolver.resolveRef(referenceUri2);
    }
}
