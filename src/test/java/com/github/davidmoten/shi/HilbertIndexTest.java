package com.github.davidmoten.shi;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.davidmoten.hilbert.HilbertCurve;
import org.davidmoten.hilbert.Ranges;
import org.davidmoten.hilbert.SmallHilbertCurve;
import org.davidmoten.kool.Stream;
import org.davidmoten.kool.StreamIterable;
import org.junit.Test;

import com.github.davidmoten.bigsorter.Reader;
import com.github.davidmoten.bigsorter.Serializer;

public class HilbertIndexTest {

    private static final double PRECISION = 0.00001;
    private static final File OUTPUT = new File("target/output");
    private static final Serializer<byte[]> SERIALIZER = Serializer.fixedSizeRecord(35);

    @Test
    public void testSimple() throws IOException {
        Serializer<String> ser = Serializer.linesUtf8();
        String s = "10,2,300\n4,5,600\n8,7,100";
        File input = new File("target/input");
        Files.write(input.toPath(), s.getBytes(StandardCharsets.UTF_8));
        Function<String, double[]> point = line -> Arrays //
                .stream(line.split(",")) //
                .mapToDouble(x -> Double.parseDouble(x)) //
                .toArray();
        int bits = 2;
        int dimensions = 3;
        int approxNumIndexEntries = 2;
        Index index = HilbertIndex.sortAndCreateIndex(input, ser, point, OUTPUT, bits, dimensions,
                approxNumIndexEntries);
        assertArrayEquals(new double[] { 4, 2, 100 }, index.mins(), PRECISION);
        assertArrayEquals(new double[] { 10, 7, 600 }, index.maxes(), PRECISION);
        assertEquals(3, index.count());
        System.out.println(index.indexPositions());
        System.out.println(new String(Files.readAllBytes(OUTPUT.toPath())));
        SmallHilbertCurve hc = HilbertCurve.small().bits(bits).dimensions(dimensions);
        // check hc calc of first item
        int firstIndex = (int) hc.index(//
                Math.round((4 - 4.0) / (10 - 4) * hc.maxOrdinate()), //
                Math.round((5 - 2.0) / (7 - 2) * hc.maxOrdinate()), //
                Math.round((600 - 100.0) / (600 - 100) * hc.maxOrdinate()));
        assertEquals(17, firstIndex);
        Map<Integer, Long> map = new HashMap<>();
        map.put(17, 0L);
        map.put(35, 8L);
        map.put(56, 16L);
        assertEquals(map, index.indexPositions());
        {
            // query to get 2nd record from sorted input 8,7,100
            long[] a = index.ordinates(7, 6, 50);
            long[] b = index.ordinates(9, 8, 150);
            Ranges ranges = index.hilbertCurve().query(a, b);
            System.out.println(ranges);
            List<PositionRange> prs = index.getPositionRanges(ranges);
            System.out.println(prs);
            assertEquals(1, prs.size());
            PositionRange pr = prs.get(0);
            assertEquals(0, pr.floorPosition());
            assertEquals(8, pr.ceilingPosition());
        }
        {
            // query outside entire domain
            long[] a = index.ordinates(3, 1, 50);
            long[] b = index.ordinates(11, 8, 650);
            Ranges ranges = index.hilbertCurve().query(a, b);
            System.out.println(ranges);
            List<PositionRange> prs = index.getPositionRanges(ranges);
            System.out.println(prs);
            PositionRange pr = prs.get(0);
            assertEquals(0, pr.floorPosition());
            assertEquals(16, pr.ceilingPosition());
        }
    }

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
                    expectedFound++;
                }
                pos += 35;
            }
        }

        long[] o1 = ind.ordinates(sb.mins());
        long[] o2 = ind.ordinates(sb.maxes());
        Ranges ranges = ind.hilbertCurve().query(o1, o2);
        List<PositionRange> positionRanges = ind.getPositionRanges(ranges);
        try (RandomAccessFile raf = new RandomAccessFile(OUTPUT, "r")) {
            List<Record> list = Stream //
                    .from(positionRanges) //
                    .flatMap(pr -> stream(sb, raf, pr)) //
                    .toList() //
                    .get();
            assertEquals(expectedFound, list.size());
        }

    }

    private StreamIterable<Record> stream(Bounds sb, RandomAccessFile raf, PositionRange pr)
            throws IOException {
        raf.seek(pr.floorPosition());
        InputStream[] in = new InputStream[1];
        final Reader<byte[]> r;
        try {
            in[0] = new LimitingInputStream(Channels.newInputStream(raf.getChannel()),
                    pr.ceilingPosition() - pr.floorPosition());
            r = SERIALIZER.createReader(in[0]);
        } catch (Throwable t) {
            if (in[0] != null) {
                in[0].close();
            }
            return Stream.error(t);
        }
        return Stream.<Record>generate(emitter -> {
            byte[] b;
            while (true) {
                b = r.read();
                if (b == null) {
                    emitter.onComplete();
                    break;
                }
                Record rec = Record.read(b);
                if (sb.contains(rec.toArray())) {
                    emitter.onNext(rec);
                    break;
                }
            }
        }).doOnDispose(() -> {
            try {
                r.close();
            } catch (Throwable t) {
                // ignore
            }
            in[0].close();
        });

    }

    private static Index createIndex() throws IOException {
        int bits = 10;
        int dimensions = 3;
        File input = new File(
                "src/test/resources/2019-05-15.binary-fixes-with-mmsi.sampled.every.400");
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
        assertArrayEquals(new double[] { -54.669193267822266, 19.543855667114258, 1.557869714E12 },
                index.mins(), 0.00001);
        assertArrayEquals(new double[] { 45.95529556274414, 179.8942413330078, 1.5579648E12 },
                index.maxes(), 0.00001);
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
