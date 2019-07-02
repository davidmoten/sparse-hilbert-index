package com.github.davidmoten.shi;

import java.nio.ByteBuffer;

public final class Record {
    public final float lat;
    public final float lon;
    public final long time;

    Record(float lat, float lon, long time) {
        this.lat = lat;
        this.lon = lon;
        this.time = time;
    }

    public double[] toArray() {
        return new double[] { lat, lon, time };
    }

    @Override
    public String toString() {
        return "Record [lat=" + lat + ", lon=" + lon + ", time=" + time + "]";
    }

    public static Record read(byte[] x) {
        ByteBuffer bb = ByteBuffer.wrap(x);
        // skip mmsi
        bb.position(4);
        float lat = bb.getFloat();
        float lon = bb.getFloat();
        long t = bb.getLong();
        return new Record(lat, lon, t);
    }
}