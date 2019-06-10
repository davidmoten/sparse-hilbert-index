package com.github.davidmoten.shi;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.function.Function;

import com.github.davidmoten.bigsorter.Serializer;

class Fixes {
    final static File input = new File(
            System.getProperty("user.home") + "/Downloads/2019-05-15.binary-fixes-with-mmsi");
    final static File output = new File("target/sorted");
    final static File idx = new File("target/sorted.idx");
    final static int recordSize = 35;
    final static Serializer<byte[]> ser = Serializer.fixedSizeRecord(recordSize);
    final static Function<byte[], double[]> point = b -> {
        ByteBuffer bb = ByteBuffer.wrap(b);
        bb.position(4);
        float lat = bb.getFloat();
        float lon = bb.getFloat();
        long time = bb.getLong();
        return new double[] { lat, lon, time };
    };
}
