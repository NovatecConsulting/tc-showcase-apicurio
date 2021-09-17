package de.novatec.serde.registry;

public class NoSuchChannelException extends Exception {

    public NoSuchChannelException() { }

    public NoSuchChannelException(String message) {
        super(message);
    }
}
