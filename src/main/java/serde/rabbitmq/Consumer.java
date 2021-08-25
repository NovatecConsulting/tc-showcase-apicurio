package serde.rabbitmq;

import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

public class Consumer extends BaseClient implements AMQPConsumer {
    private static final Logger log = Logger.getLogger(Consumer.class.getName());
    private String consumerTag;

    public Consumer(RabbitMQConfig rabbitMQConfig, java.util.function.Consumer<byte[]> messageHandler)
            throws IOException, TimeoutException {
        super(rabbitMQConfig, messageHandler);
        prepareMessageExchange();
    }

    /**
     * Defines a callback-behaviour to process arriving messages which is executed as soon as a new message
     * is available on the specified queue.
     */
    @Override
    public void consumeMessages() {
        try {
            log.info(" ... Waiting for messages. To exit press CTRL+C");
            getChannel().basicQos(1);

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                log.info("Received message.");
                getMessageHandler().accept(delivery.getBody());
                getChannel().basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            };
            consumerTag = getChannel().basicConsume(getQueueName(), false, deliverCallback, consumerTag -> {
            });
        } catch (IOException e) {
            log.warning("Message could not be consumed and acknowledged.");
        }
    }

    /**
     * Declare a new queue on this session/channel if the queue was not already created.
     * @throws IOException if queue could not be declared
     */
    @Override
    public void prepareMessageExchange() throws IOException {
        getChannel().queueDeclare(getQueueName(), false, false, false, null);
    }

    /**
     * Cancel the message consumption and close the connection.
     * @throws IOException
     */
    @Override
    public void stop() throws IOException {
        log.info("Stopping client...");
        getChannel().basicCancel(consumerTag);
        getConnection().close();
    }
}
