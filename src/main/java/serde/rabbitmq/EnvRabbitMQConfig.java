package serde.rabbitmq;

import java.util.Map;

public class EnvRabbitMQConfig implements RabbitMQConfig {

    public static final String HOST_VAR = "HOST";
    public static final String PORT_VAR = "PORT";
    public static final String MANAGEMENTPORT_VAR = "MANAGEMENT_PORT";
    public static final String USER_VAR = "USER";
    public static final String PASSWORD_VAR = "PASSWORD";
    public static final String QUEUE_NAME_VAR = "QUEUE_NAME";
    public static final String EXCHANGE_NAME_VAR = "EXCHANGE_NAME";

    private final Map<String, String> env;

    public EnvRabbitMQConfig() {
        this(System.getenv());
    }

    public EnvRabbitMQConfig(Map<String, String> env) {
        this.env = env;
    }

    @Override
    public String getHost() {
        return env.getOrDefault(HOST_VAR, "localhost");
    }

    @Override
    public int getPort() {
        return Integer.parseInt(env.getOrDefault(PORT_VAR, "5672"));
    }

    @Override
    public int getManagementPort() {
        return Integer.parseInt(env.getOrDefault(MANAGEMENTPORT_VAR, "15672"));
    }

    @Override
    public String getUser() {
        return env.getOrDefault(USER_VAR, "guest");
    }

    @Override
    public String getPassword() {
        return env.getOrDefault(PASSWORD_VAR, "guest");
    }

    @Override
    public String getQueueName() {
        return env.getOrDefault(QUEUE_NAME_VAR, "task_queue");
    }

    @Override
    public String getExchangeName() {
        return env.getOrDefault(EXCHANGE_NAME_VAR, "task_exchange");
    }
}
