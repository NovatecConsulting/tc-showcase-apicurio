import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.apicurio.datamodels.Library;
import io.apicurio.datamodels.asyncapi.models.AaiChannelItem;
import io.apicurio.datamodels.asyncapi.models.AaiDocument;
import io.apicurio.datamodels.core.models.Document;
import io.apicurio.datamodels.core.models.Node;
import io.apicurio.registry.rest.client.RegistryClient;
import io.apicurio.registry.rest.client.RegistryClientFactory;
import io.apicurio.registry.rest.v2.beans.IfExists;
import io.apicurio.registry.types.ArtifactType;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import resolver.AbsoluteAvroReference;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class ApiRegistry {
    private final String REGISTRY_URL;

    public ApiRegistry(String registryUrl) {
        REGISTRY_URL = registryUrl;
    }

    public void registerArtifact(String artifactID, String apiResource, ArtifactType type) throws IOException {
        try(InputStream input = ProducerApplication.class.getResource(apiResource).openStream()) {
            RegistryClient client = RegistryClientFactory.create(REGISTRY_URL);
            //InputStream input = IOUtils.toInputStream(apiResource, StandardCharsets.UTF_8);
            client.createArtifact("default", artifactID, type, IfExists.UPDATE, input);
        }
    }

    /*
    TODO get Api definition via REST call
     */
    public Document getApiDefinition(String apiResource) throws IOException {
        InputStream inputStream = ProducerApplication.class.getResource(apiResource).openStream();
        ObjectMapper o = new ObjectMapper();
        JsonNode node =  o.readTree(inputStream);

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
