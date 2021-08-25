package serde.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public abstract class BaseClient {
    private RabbitMQConfig rabbitMQConfig;
    private java.util.function.Consumer<byte[]> messageHandler;
    private Channel channel;
    private Connection connection;

    public BaseClient(RabbitMQConfig rabbitMQConfig, java.util.function.Consumer<byte[]> messageHandler)
            throws IOException, TimeoutException {
        this.rabbitMQConfig = rabbitMQConfig;
        this.messageHandler = messageHandler;
        initializeClient();
    }

    public BaseClient(RabbitMQConfig rabbitMQConfig) throws IOException, TimeoutException {
        this(rabbitMQConfig, null);
    }

    private void initializeClient() throws IOException, TimeoutException {
        createConnection();
        createSession();
        prepareMessageExchange();
    }

    private void createConnection() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(rabbitMQConfig.getHost());
        factory.setPort(rabbitMQConfig.getPort());
        connection = factory.newConnection();
    }

    private void createSession() throws IOException {
        if(connection != null) {
            channel = connection.createChannel();
        }
    }

    public Channel getChannel() {
        return channel;
    }

    public Connection getConnection() {
        return connection;
    }

    public String getQueueName() {
        return rabbitMQConfig.getQueueName();
    }

    public String getExchangeName() {
        return rabbitMQConfig.getExchangeName();
    }

    public java.util.function.Consumer<byte[]> getMessageHandler() {
        return messageHandler;
    }

    protected abstract void prepareMessageExchange() throws IOException;
}
