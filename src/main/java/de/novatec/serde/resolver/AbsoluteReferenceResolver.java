package de.novatec.serde.resolver;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import com.fasterxml.jackson.databind.JsonNode;
import io.apicurio.datamodels.core.models.Node;

/**
 * Resolves an external HTTP reference to a {@link Node}.  The algorithm is:
 *
 * 1) Fetch HTTP content to a String representing the external resource
 * 2) Parse the external resource using Jackson
 * 3) Resolve a {@link JsonNode} using the "path" section of the URI (the part after the #)
 * 4) Parse the resulting {@link JsonNode} to a data model {@link Node}
 *
 * An example of a reference resolvable by this class is:
 *
 *   https://www.example.com/types/example-types.json#/schemas/FooBarType
 *
 * This will download the content found at https://www.example.com/types/example-types.json
 * and then return any entity found at path "/schemas/FooBarType".
 *
 * @author eric.wittmann@gmail.com
 */

public class AbsoluteReferenceResolver extends AbstractReferenceResolver {

    private static CloseableHttpClient httpClient = HttpClients.createDefault();

    /**
     * Constructor.
     */
    public AbsoluteReferenceResolver() {
    }


    @Override
    protected boolean accepts(URI uri) {
        String scheme = uri.getScheme();
        return scheme != null && scheme.toLowerCase().startsWith("http");
    }


    @Override
    protected String fetchUriContent(URI referenceUri) throws IOException {
        HttpGet get = new HttpGet(referenceUri);
        try (CloseableHttpResponse response = httpClient.execute(get)) {
            if (response.getStatusLine().getStatusCode() < 200 || response.getStatusLine().getStatusCode() > 299) {
                return null;
            }
            try (InputStream contentStream = response.getEntity().getContent()) {
                ContentType ct = ContentType.getOrDefault(response.getEntity());
                String encoding = StandardCharsets.UTF_8.name();
                if (ct != null && ct.getCharset() != null) {
                    encoding = ct.getCharset().name();
                }
                return IOUtils.toString(contentStream, encoding);
            }
        }
    }

}


