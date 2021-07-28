package common;

public interface RabbitMQConfig {

    String getHost();

    int getPort();

    int getManagementPort();

    String getUser();

    String getPassword();

    String getQueueName();

    String getExchangeName();
}
