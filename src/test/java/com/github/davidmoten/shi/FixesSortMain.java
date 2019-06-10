package com.github.davidmoten.shi;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.github.davidmoten.bigsorter.Serializer;

public class FixesSortMain {

    public static void main(String[] args) {
        File input = new File(System.getProperty("user.home")
                + "/Downloads/2019-05-15.binary-fixes-with-mmsi.gz");
        File output = new File("target/sorted");
        File idx = new File("target/sorted.idx");
        int recordSize = 35;
        Serializer<byte[]> ser = Serializer.fixedSizeRecord(recordSize);
        Function<byte[], double[]> point = b -> {
            ByteBuffer bb = ByteBuffer.wrap(b);
            bb.position(4);
            float lat = bb.getFloat();
            float lon = bb.getFloat();
            long time = bb.getLong();
            return new double[] { lat, lon, time };
        };
        Index<byte[]> index = Index//
                .serializer(ser) //
                .pointMapper(point) //
                .inputGzipped(input) //
                .output(output) //
                .bits(10) //
                .dimensions(3) //
                .createIndex() //
                .write(idx);
        System.out.println(index);
        double minTime = 1.557868858E12;
        double maxTime = 1.5579648E12;
        double t = minTime + TimeUnit.HOURS.toMillis(12);
        double[] a = new double[] { -33.68, 151.02, t };
        double[] b = new double[] { -34.06, 151.34, t + TimeUnit.HOURS.toMillis(1) };
        long count = index.search(Bounds.create(a, b), output).count().get();
        System.out.println(count);
    }

}