package com.github.davidmoten.shi;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.davidmoten.hilbert.Ranges;
import org.davidmoten.kool.Stream;
import org.junit.Test;

import com.github.davidmoten.bigsorter.Reader;
import com.github.davidmoten.bigsorter.Serializer;

public class HilbertIndexTest {

    private static final File OUTPUT = new File("target/output");
    private static final Serializer<byte[]> SERIALIZER = Serializer.fixedSizeRecord(35);

    @Test
    public void testCalculationOfIndex() throws FileNotFoundException, IOException {
        Index index = createIndex();

        System.out.println(index);
        System.out.println(new Date(Math.round(index.mins()[2])));

        checkIndex(index);
    }

    @Test
    public void testIndexSerializationRoundTrip() throws IOException {
        Index index = createIndex();

        File idx = new File("target/output.idx");

        // check serialization
        index.write(idx);

        // reread index
        index = Index.read(idx);
        checkIndex(index);
    }

    @Test
    public void testQuery() throws IOException {
        Index ind = createIndex();
        Bounds sb = createQueryBounds(Math.round(ind.mins()[2]), Math.round(ind.maxes()[2]));
        int expectedFound = 0;
        {
            Reader<byte[]> r = SERIALIZER.createReader(Util.bufferedInput(OUTPUT));
            byte[] b;
            long pos = 0;
            while ((b = r.read()) != null) {
                Record rec = Record.read(b);
                if (sb.contains(rec.toArray())) {
                    System.out.println("found " + pos + ": " + rec);
                    expectedFound++;
                }
                pos += 35;
            }
        }

        long[] o1 = ind.ordinates(sb.mins());
        long[] o2 = ind.ordinates(sb.maxes());
        Ranges ranges = ind.hilbertCurve().query(o1, o2);
        System.out.println(ranges.size() + ": " + ranges);
        List<PositionRange> positionRanges = ind.getPositionRanges(ranges);
        System.out.println("simplifiedPositionRanges:");
        positionRanges.forEach(System.out::println);
        try (RandomAccessFile raf = new RandomAccessFile(OUTPUT, "r")) {
            List<Record> list = Stream //
                    .from(positionRanges) //
                    .flatMap(pr -> {
                        System.out.println("floor=" + pr.floorPosition() + ",ceiling=" + pr.ceilingPosition());
                        raf.seek(pr.floorPosition());
                        List<Record> recs = new ArrayList<>();
                        while (raf.getFilePointer() <= pr.ceilingPosition()) {
                            byte[] b = new byte[35];
                            raf.readFully(b);
                            Record rec = Record.read(b);
                            if (sb.contains(rec.toArray())) {
                                recs.add(rec);
                            }
                        }
                        return Stream.from(recs);
                        // try (InputStream in = new
                        // LimitingInputStream(Channels.newInputStream(raf.getChannel()),
                        // pr.ceilingPosition() - pr.floorPosition());
                        // Reader<byte[]> r = SERIALIZER.createReader(in)) {
                        // byte[] b;
                        // while ((b = r.read()) != null) {
                        // Record rec = Record.read(b);
                        // if (sb.contains(rec.toArray())) {
                        // recs.add(rec);
                        // }
                        // }
                        // return Stream.from(recs);
                        // }
                    }) //
                    .toList() //
                    .get();
            assertEquals(expectedFound, list.size());
        }

    }

    private static Index createIndex() throws IOException {
        int bits = 10;
        int dimensions = 3;
        File input = new File("src/test/resources/2019-05-15.binary-fixes-with-mmsi.sampled.every.400");
        Function<byte[], double[]> point = b -> {
            Record rec = Record.read(b);
            return new double[] { rec.lat, rec.lon, rec.time };
        };
        int approximateNumIndexEntries = 100;
        return HilbertIndex.sortAndCreateIndex(input, SERIALIZER, point, OUTPUT, bits, dimensions,
                approximateNumIndexEntries);
    }

    private void checkIndex(Index index) {
        assertEquals(29163, index.count());
        assertArrayEquals(new double[] { -54.669193267822266, 19.543855667114258, 1.557869714E12 }, index.mins(),
                0.00001);
        assertArrayEquals(new double[] { 45.95529556274414, 179.8942413330078, 1.5579648E12 }, index.maxes(), 0.00001);
        assertEquals(102, index.numEntries());
    }

    private static Bounds createQueryBounds(long minTime, long maxTime) {
        float lat1 = -30.0f;
        float lon1 = 100f;
        long t1 = 1557892719000L - TimeUnit.MINUTES.toMillis(30);
        float lat2 = -35;
        float lon2 = 120f;
        long t2 = t1 + TimeUnit.HOURS.toMillis(1);
        return new Bounds(new double[] { lat1, lon1, t1 }, new double[] { lat2, lon2, t2 });
    }

}
