package de.novatec.serde.registry;

public class NoPayloadException extends Exception {

    public NoPayloadException() { }

    public NoPayloadException(String message) {
        super(message);
    }
}
