package resolver;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;

import com.fasterxml.jackson.core.JsonProcessingException;
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

    /**
     * Returns true if this resolver accepts the given URI, indicating that this resolver
     * is capable of resolving the reference.
     * @param uri
     */
    protected abstract boolean accepts(URI uri);

    /**
     * Resolves an HTTP URI reference.  See the class javadoc for algorithm details.
     * @param referenceUri
     * @throws IOException
     * @throws MalformedURLException
     */
    protected Schema resolveUriRef(URI referenceUri) throws IOException {
        String externalContent = fetchUriContent(referenceUri);
        if (externalContent == null) {
            return null;
        }
        Schema externalContentRoot = parseUriContent(externalContent);
        if (externalContentRoot == null) {
            return null;
        }
        return externalContentRoot;
    }

    /**
     * Fetch the content at the given URI using some sort of implementation-specific approach.
     * @param referenceUri
     * @throws IOException
     * @throws MalformedURLException
     */
    protected abstract String fetchUriContent(URI referenceUri) throws IOException;

    /**
     * Parse the given external content.  The content needs to be in Avro Format.
     *
     * TODO: Add support/distinction between JSON and YAML
     *
     * @param externalContent
     */
    protected Schema parseUriContent(String externalContent) throws JsonProcessingException {
        System.out.println('"' + externalContent+ '"');
        return new Schema.Parser().parse(externalContent);
    }
}
