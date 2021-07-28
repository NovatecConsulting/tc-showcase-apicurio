import common.AMQPProducer;
import common.RabbitMQConfig;
import common.Stoppable;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

import static com.rabbitmq.client.MessageProperties.MINIMAL_BASIC;
import static com.rabbitmq.client.MessageProperties.TEXT_PLAIN;
import static java.nio.charset.StandardCharsets.UTF_8;

public class Producer extends BaseClient implements AMQPProducer, Stoppable {
    private static final Logger log = Logger.getLogger(Producer.class.getName());

    public Producer(RabbitMQConfig rabbitMQConfig) throws IOException, TimeoutException {
        super(rabbitMQConfig);
        prepareMessageExchange();
    }

    /**
     * Sends a String message to the default exchange using the queue name as routing key.
     * @param message message that should be sent to the broker
     */
    @Override
    public void sendStringMessage(String message) {
        try {
            getChannel().basicPublish("", getQueueName(), TEXT_PLAIN, message.getBytes(UTF_8));
            System.out.println("Sent '" + message + "'");
        } catch (IOException e) {
            log.warning( "Message could not be delivered.");
        }
    }

    public void sendByteMessage(byte[] message) {
        try {
            getChannel().basicPublish("", getQueueName(), MINIMAL_BASIC, message);
            System.out.println("Sent byte-array message.");
        } catch (IOException e) {
            log.warning( "Message could not be delivered.");
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

    @Override
    public void stop() throws IOException {
        log.info("Stopping client...");
        getConnection().close();
    }
}
