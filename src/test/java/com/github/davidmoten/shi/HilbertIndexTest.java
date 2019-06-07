package com.github.davidmoten.shi;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.Test;

import com.github.davidmoten.bigsorter.Serializer;

public class HilbertIndexTest {

    @Test
    public void testCalculationOfIndex() throws FileNotFoundException, IOException {
        Index index = createIndex();

        System.out.println(index);
        System.out.println(new Date(Math.round(index.mins()[2])));

        checkIndex(index);
    }

    @Test
    public void testIndexSerializationRoundTrip() throws FileNotFoundException, IOException {
        Index index = createIndex();

        File idx = new File("target/output.idx");

        // check serialization
        index.write(idx);

        // reread index
        index = Index.read(idx);
        checkIndex(index);
    }

    private static Index createIndex() throws FileNotFoundException, IOException {
        int bits = 10;
        int dimensions = 3;
        File input = new File("src/test/resources/2019-05-15.binary-fixes-with-mmsi.sampled.every.400");
        Serializer<byte[]> serializer = Serializer.fixedSizeRecord(35);
        Function<byte[], double[]> point = b -> {
            Record rec = Record.read(b);
            return new double[] { rec.lat, rec.lon, rec.time };
        };
        File output = new File("target/output");

        int approximateNumIndexEntries = 100;
        return HilbertIndex.sortAndCreateIndex(input, serializer, point, output, bits, dimensions,
                approximateNumIndexEntries);
    }

    private void checkIndex(Index index) {
        assertEquals(29163, index.count());
        assertArrayEquals(new double[] { -54.669193267822266, 19.543855667114258, 1.557869714E12 }, index.mins(),
                0.00001);
        assertArrayEquals(new double[] { 45.95529556274414, 179.8942413330078, 1.5579648E12 }, index.maxes(), 0.00001);
        assertEquals(102, index.numEntries());
    }

    private static Bounds createSydneyBounds(long minTime, long maxTime) {
        float lat1 = -33.806477f;
        float lon1 = 151.181767f;
        long t1 = Math.round(minTime + (maxTime - minTime) / 2);
        float lat2 = -33.882896f;
        float lon2 = 151.281330f;
        long t2 = t1 + TimeUnit.HOURS.toMillis(1);
        return new Bounds(new double[] { lat1, lon1, t1 }, new double[] { lat2, lon2, t2 });
    }

}
