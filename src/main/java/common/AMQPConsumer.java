package common;

import java.io.IOException;

public interface AMQPConsumer extends Stoppable {
    /**
     * Opens a new thread for message consumption and waits for new messages (event-driven) or
     * actively polls for new messages (polling).
     */
    void consumeMessages() throws IOException;
}
