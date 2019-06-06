package com.github.davidmoten.shi;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.function.Function;

import org.junit.Test;

import com.github.davidmoten.bigsorter.Serializer;

public class HilbertIndexTest {

    @Test
    public void test() throws FileNotFoundException, IOException {
        int bits = 10;
        int dimensions = 3;
        File input = new File("src/test/resources/2019-05-15.binary-fixes-with-mmsi.sampled.1000");
        Serializer<byte[]> serializer = Serializer.fixedSizeRecord(35);
        Function<byte[], double[]> point = b -> {
            Record rec = Record.read(b);
            return new double[] { rec.lat, rec.lon, rec.time };
        };
        File output = new File("target/output");
        File idx = new File("target/output.idx");
        HilbertIndex.sortAndCreateIndex(input, serializer, point, output, idx, bits, dimensions);
    }

}
