package ru.mail.polis.alexeykotelevskiy;

public class KeyInfo {
    private final State state;
    private final long timestamp;
    private final byte[] value;

    public KeyInfo(State state, long timestamp, byte[] value) {
        this.state = state;
        this.timestamp = timestamp;
        this.value = value;
    }

    public State getState() {
        return state;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public byte[] getValue() {
        return value;
    }

}
