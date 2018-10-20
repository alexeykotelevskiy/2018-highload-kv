package ru.mail.polis.alexeykotelevskiy;

import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.Arrays;

public class ValueNode implements Serializable {
    private final byte[] val;
    private Long timestamp;
    private boolean milestone;

    public ValueNode(byte[] val, Long timestamp, boolean milestone) {
        this.val = val;
        this.timestamp = timestamp;
        this.milestone = milestone;
    }

    public boolean isMilestone() {
        return milestone;
    }

    public byte[] getVal() {
        return val;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public void setMilestone(boolean milestone) {
        this.milestone = milestone;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(val);
    }
}
