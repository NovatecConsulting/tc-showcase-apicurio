package serde.rabbitmq;

import java.io.IOException;

public interface AMQPProducer extends Stoppable {

    /**
     * Sends a given String as AMQP message to the broker. The message can be in AMQP 0.9.1 or 1.0 format, but
     * compatibility can not be ensured in both directions.
     * @param message to be sent
     */
    void sendStringMessage(String message) throws IOException;
}
