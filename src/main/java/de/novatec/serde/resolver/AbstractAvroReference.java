package de.novatec.serde.resolver;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractAvroReference {

    private static final Logger logger = LoggerFactory.getLogger(AbstractAvroReference.class);

    public Schema resolveRef(String reference) {
        try {
            URI uri = new URI(reference);
            if (accepts(uri)) {
                return resolveUriRef(uri);
            }
            return null;
        } catch (Exception e) {
            logger.error("Error resolving external reference", e);
            return null;
        }
    }

    protected abstract boolean accepts(URI uri);

    protected Schema resolveUriRef(URI referenceUri) throws IOException {
        String externalContent = fetchUriContent(referenceUri);
        if (externalContent == null) {
            return null;
        }

        //distinguish between Schema Registry and Apicurio Registry
        Schema externalContentRoot = null;
        if(referenceUri.getPort() == 8080) {
            externalContentRoot = parseApicurioRegistryUriContent(externalContent);
        }else if(referenceUri.getPort() == 8081) {
            externalContentRoot = parseSchemaRegistryUriContent(externalContent);
        }else {
            System.out.println("TODO Exception");
        }

        if (externalContentRoot == null) {
            return null;
        }
        return externalContentRoot;
    }

    protected abstract String fetchUriContent(URI referenceUri) throws IOException;

    protected Schema parseSchemaRegistryUriContent(String externalContent) throws IOException {
        InputStream inputStream = new ByteArrayInputStream(externalContent.getBytes());
        JsonNode node =  new ObjectMapper().readTree(inputStream);
        return new Schema.Parser().parse(node.get("schema").asText());
    }

    protected Schema parseApicurioRegistryUriContent(String externalContent) {
        return new Schema.Parser().parse(externalContent);
    }
}
