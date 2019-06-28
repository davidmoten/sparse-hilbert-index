package com.github.davidmoten.shi;

import java.text.DecimalFormat;

public final class WithStats<T> {

    private final T value;
    private final long recordsRead;
    private final long bytesRead;
    private final long recordsFound;
    private final long timeToFirstByte;
    private final long chunksRead;
    private final long elapsedTime;

    WithStats(T value, long recordsRead, long recordsFound, long bytesRead, long timeToFirstByte,
            long chunksRead, long elapsedTime) {

        this.value = value;
        this.recordsRead = recordsRead;
        this.recordsFound = recordsFound;
        this.bytesRead = bytesRead;
        this.timeToFirstByte = timeToFirstByte;
        this.chunksRead = chunksRead;
        this.elapsedTime = elapsedTime;
    }

    public boolean hasValue() {
        return value != null;
    }

    public T value() {
        return value;
    }

    public long recordsRead() {
        return recordsRead;
    }

    public double hitRatio() {
        return (double) recordsFound / recordsRead;
    }

    public long bytesRead() {
        return bytesRead;
    }

    public long recordsFound() {
        return recordsFound;
    }

    public long timeToFirstByteMs() {
        return timeToFirstByte;
    }

    public double timeToFirstByteMsAverage() {
        return (double) timeToFirstByte / chunksRead;
    }

    public long chunksRead() {
        return chunksRead;
    }
    
    public long elapsedTimeMs() {
        return elapsedTime;
    }

    @Override
    public String toString() {
        DecimalFormat df = new DecimalFormat("0.0000");
        StringBuilder b = new StringBuilder();
        b.append("WithStats [");
        b.append("elapsedMs=");
        b.append(elapsedTime);
        b.append(", recordsFound=");
        b.append(recordsFound);
        b.append(", recordsRead=");
        b.append(recordsRead);
        b.append(", hitRatio=");
        b.append(df.format(hitRatio()));
        b.append(", bytesRead=");
        b.append(bytesRead);
        b.append(", timeToFirstByteMsTotal=");
        b.append(timeToFirstByte);
        b.append(", timeToFirstByteMsAverage=");
        b.append(df.format(timeToFirstByteMsAverage()));
        b.append(", chunksRead=");
        b.append(chunksRead);
        b.append("]");
        return b.toString();
    }

}
