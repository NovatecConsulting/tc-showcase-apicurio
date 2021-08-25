package serde.rabbitmq;

import java.io.IOException;

public interface Stoppable {
    /**
     * Gracefully stops the client and closes the connection to the broker.
     * @throws IOException
     */
    void stop() throws IOException, InterruptedException;
}
