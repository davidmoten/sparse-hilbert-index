package com.github.davidmoten.shi.fixes;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.github.davidmoten.bigsorter.Serializer;
import com.github.davidmoten.shi.Bounds;

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
    private static double minTime = 1.557868858E12;
    private static double t1 = minTime + TimeUnit.HOURS.toMillis(0);
    private static double t2 = t1 + TimeUnit.MINUTES.toMillis(24*60);
    final static Bounds sydney = Bounds.create(new double[] { -33.68, 150.86, t1 },
            new double[] { -34.06, 151.34, t2 });
    final static Bounds brisbane = Bounds.create(new double[] { -24.9, 150, t1 },
            new double[] { -29.5, 158, t2 });
    final static Bounds qld = Bounds.create(new double[] { -9.481, 137.3, t1 },
            new double[] { -29.0, 155.47, t2 });
}
