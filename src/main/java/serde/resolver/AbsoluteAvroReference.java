package serde.resolver;

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

public class AbsoluteAvroReference extends AbstractAvroReference {
    private static CloseableHttpClient httpClient = HttpClients.createDefault();

    public AbsoluteAvroReference() {

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
