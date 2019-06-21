package com.github.davidmoten.shi;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.davidmoten.hilbert.Range;
import org.davidmoten.hilbert.Ranges;
import org.davidmoten.hilbert.SmallHilbertCurve;
import org.davidmoten.kool.Stream;
import org.davidmoten.kool.exceptions.UncheckedException;
import org.junit.Test;

import com.github.davidmoten.bigsorter.Reader;
import com.github.davidmoten.bigsorter.Serializer;
import com.github.davidmoten.guavamini.Lists;
import com.github.davidmoten.shi.fixes.Record;

public class IndexTest {

    private static final Bounds SIMPLE_BOUNDS_WHOLE_DOMAIN = Bounds.create(new double[] { 3, 1, 50 },
            new double[] { 11, 8, 650 });
    private static final Serializer<String> SIMPLE_SERIALIZER = Serializer.linesUtf8();
    private static final Function<String, double[]> SIMPLE_POINT_MAPPER = line -> Arrays //
            .stream(line.split(",")) //
            .mapToDouble(x -> Double.parseDouble(x)) //
            .toArray();
    private static final int NUM_SIMPLE_ROWS = 3;
    private static final double PRECISION = 0.00001;
    private static final File OUTPUT = new File("target/output");
    private static final Serializer<byte[]> SERIALIZER = Serializer.fixedSizeRecord(35);
    private static final Function<byte[], double[]> POINT_FN = b -> {
        Record rec = Record.read(b);
        return new double[] { rec.lat, rec.lon, rec.time };
    };

    @Test
    public void test() {
        TreeMap<Integer, Long> map = new TreeMap<>();
        map.put(237, 0L);
        map.put(472177237, 4082820L);
        Index<String> index = new Index<String>(map, new double[] { -85.14174, -115.24912, 1557868858000L },
                new double[] { 47.630283, 179.99948, 1557964800000L }, 10, 2, Serializer.linesUtf8(),
                x -> new double[] { 0, 0, 0 });
        long[] o = index.ordinates(-84.23007, -115.24912, 1557964123000L);
        assertEquals(153391853, index.hilbertCurve().index(o));
    }

    @Test
    public void testGetPositionRangesForEmptyRanges() {
        TreeMap<Integer, Long> map = new TreeMap<>();
        map.put(1, 0L);
        map.put(8, 5L);
        List<PositionRange> ranges = Index.positionRanges(map, Collections.emptyList());
        assertTrue(ranges.isEmpty());
    }

    @Test
    public void testGetPositionRangesSingleRangeAboveMax() {
        TreeMap<Integer, Long> map = new TreeMap<>();
        map.put(1, 0L);
        map.put(8, 5L);
        List<PositionRange> ranges = Index.positionRanges(map, Collections.singletonList(Range.create(10, 12)));
        assertTrue(ranges.isEmpty());
    }

    @Test
    public void testGetPositionRangesSingleRangeBelowMin() {
        TreeMap<Integer, Long> map = new TreeMap<>();
        map.put(1, 0L);
        map.put(8, 5L);
        List<PositionRange> ranges = Index.positionRanges(map, Collections.singletonList(Range.create(-3, -1)));
        assertTrue(ranges.isEmpty());
    }

    @Test
    public void testGetPositionRanges1() {
        TreeMap<Integer, Long> map = new TreeMap<>();
        map.put(1, 0L);
        map.put(8, 5L);
        map.put(16, 10L);
        map.put(20, 16L);
        List<PositionRange> ranges = Index.positionRanges(map, Collections.singletonList(Range.create(5, 9)));
        System.out.println(ranges);
        assertEquals(1, ranges.size());
        PositionRange pr = ranges.get(0);
        assertEquals(0, pr.floorPosition());
        assertEquals(10, pr.ceilingPosition());
    }

    @Test
    public void testGetPositionRanges2() {
        TreeMap<Integer, Long> map = new TreeMap<>();
        map.put(1, 0L);
        map.put(8, 5L);
        map.put(16, 10L);
        map.put(20, 16L);
        List<PositionRange> ranges = Index.positionRanges(map,
                Lists.newArrayList(Range.create(2, 3), Range.create(18, 22)));
        System.out.println(ranges);
        assertEquals(2, ranges.size());
        {
            PositionRange pr = ranges.get(0);
            assertEquals(0, pr.floorPosition());
            assertEquals(5, pr.ceilingPosition());
        }
        {
            PositionRange pr = ranges.get(1);
            assertEquals(10, pr.floorPosition());
            assertEquals(Long.MAX_VALUE, pr.ceilingPosition());
        }
    }

    @Test
    public void testSimpleGetters() throws FileNotFoundException, IOException {
        Index<String> index = createSimpleIndex();
        assertTrue(SIMPLE_SERIALIZER == index.serializer());
        assertTrue(SIMPLE_POINT_MAPPER == index.pointMapper());
    }

    @Test
    public void testSimpleSearchWithStats() throws FileNotFoundException, IOException {
        Index<String> index = createSimpleIndex();
        List<WithStats<String>> list = index //
                .search(SIMPLE_BOUNDS_WHOLE_DOMAIN) //
                .withStats() //
                .maxRanges(0) //
                .file(OUTPUT) //
                .toList() //
                .get();
        list.forEach(System.out::println);
        assertEquals(4, list.size());
    }

    @Test
    public void testSimpleSearchWithStatsUsingUrl() throws FileNotFoundException, IOException {
        Index<String> index = createSimpleIndex();
        List<WithStats<String>> list = index //
                .search(SIMPLE_BOUNDS_WHOLE_DOMAIN) //
                .withStats() //
                .maxRanges(0) //
                .rangesBufferSize(0) //
                .url(OUTPUT.toURI().toURL().toString()) //
                .toList() //
                .get();
        list.forEach(System.out::println);
        assertEquals(4, list.size());
    }

    @Test(expected = RuntimeException.class)
    public void testSimpleSearchWithStatsUsingBadUrlThrows() throws FileNotFoundException, IOException {
        Index<String> index = createSimpleIndex();
        index //
                .search(SIMPLE_BOUNDS_WHOLE_DOMAIN) //
                .withStats() //
                .maxRanges(0) //
                .url("abc://def");
    }

    @Test
    public void testSimple() throws IOException {
        Index<String> index = createSimpleIndex();
        assertArrayEquals(new double[] { 4, 2, 100 }, index.mins(), PRECISION);
        assertArrayEquals(new double[] { 10, 7, 600 }, index.maxes(), PRECISION);
        assertEquals(3, index.count());
        System.out.println(index.indexPositions());
        System.out.println(new String(Files.readAllBytes(OUTPUT.toPath())));
        SmallHilbertCurve hc = index.hilbertCurve();
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
            List<PositionRange> prs = index.positionRanges(ranges);
            System.out.println(prs);
            assertEquals(1, prs.size());
            PositionRange pr = prs.get(0);
            assertEquals(0, pr.floorPosition());
            assertEquals(16, pr.ceilingPosition());
        }
        {
            // query outside entire domain
            long[] a = index.ordinates(3, 1, 50);
            long[] b = index.ordinates(11, 8, 650);
            Ranges ranges = index.hilbertCurve().query(a, b);
            System.out.println(ranges);
            List<PositionRange> prs = index.positionRanges(ranges);
            System.out.println(prs);
            PositionRange pr = prs.get(0);
            assertEquals(0, pr.floorPosition());
            assertEquals(Long.MAX_VALUE, pr.ceilingPosition());
        }
    }

    @Test(expected = UncheckedIOException.class)
    public void testIndexReadFileDoesNotExist() {
        Index //
                .serializer(SIMPLE_SERIALIZER) //
                .pointMapper(SIMPLE_POINT_MAPPER) //
                .read(new File("target/doesnotexist"));
    }

    @Test(expected = UncheckedIOException.class)
    public void testIndexReadUrl() throws MalformedURLException {
        Index //
                .serializer(SIMPLE_SERIALIZER) //
                .pointMapper(SIMPLE_POINT_MAPPER) //
                .read(new File("target/doesnotexist").toURI().toURL());
    }

    @Test(expected = UncheckedIOException.class)
    public void testIndexCreationFileDoesNotExist() {
        Index //
                .serializer(SIMPLE_SERIALIZER) //
                .pointMapper(SIMPLE_POINT_MAPPER) //
                .input(new File("target/doesnotexist")) //
                .output(OUTPUT).bits(10) //
                .dimensions(3) //
                .createIndex();
    }

    @Test(expected = UncheckedIOException.class)
    public void testIndexWriteFileCannotBeCreated() throws FileNotFoundException, IOException {
        Index<String> index = createSimpleIndex();
        index.write(new File("target/doesnotexist/doesnotexist"));
    }

    @Test
    public void testSimpleSearchWholeDomain() throws FileNotFoundException, IOException {
        Index<String> index = createSimpleIndex();
        Bounds queryBounds = SIMPLE_BOUNDS_WHOLE_DOMAIN;
        assertEquals(NUM_SIMPLE_ROWS, index.search(queryBounds).file(OUTPUT).count().get().intValue());
        File idx2 = new File("target/idx2");
        index.write(idx2);
        Index<String> index2 = Index.serializer(SIMPLE_SERIALIZER).pointMapper(SIMPLE_POINT_MAPPER).read(idx2);
        assertEquals(NUM_SIMPLE_ROWS, index2.search(queryBounds).file(OUTPUT).count().get().intValue());
        Index<String> index3 = Index.serializer(SIMPLE_SERIALIZER).pointMapper(SIMPLE_POINT_MAPPER)
                .read(idx2.toURI().toURL());
        assertEquals(NUM_SIMPLE_ROWS, index3.search(queryBounds).file(OUTPUT).count().get().intValue());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPointMapperDoesntMatchDimensions() throws IOException {
        String s = "10,2,300\n4,5,600\n8,7,100";
        File input = new File("target/input");
        Files.write(input.toPath(), s.getBytes(StandardCharsets.UTF_8));

        int bits = 2;
        int dimensions = 3;
        int approxNumIndexEntries = 2;

        Function<String, double[]> pointMapper = x -> new double[1];
        Index //
                .serializer(SIMPLE_SERIALIZER) //
                .pointMapper(pointMapper) //
                .input(input) //
                .output(OUTPUT) //
                .bits(bits) //
                .dimensions(dimensions) //
                .numIndexEntries(approxNumIndexEntries) //
                .createIndex();
    }

    @Test
    public void testSearchWorksEvenIfChunksDontHaveDifferentHilbertIndexes() throws IOException {
        String s = "10,2,300\n10,2,300\n10,2,300";
        File input = new File("target/input");
        Files.write(input.toPath(), s.getBytes(StandardCharsets.UTF_8));

        int bits = 2;
        int dimensions = 3;
        int approxNumIndexEntries = 5;

        Index<String> idx = Index //
                .serializer(SIMPLE_SERIALIZER) //
                .pointMapper(SIMPLE_POINT_MAPPER) //
                .input(input) //
                .output(OUTPUT) //
                .bits(bits) //
                .dimensions(dimensions) //
                .numIndexEntries(approxNumIndexEntries) //
                .createIndex();
        System.out.println(idx);
        assertEquals(3, idx //
                .search(new double[] { 9, 1, 100 }, new double[] { 11, 3, 400 }) //
                .maxRanges(0) //
                .rangesBufferSize(0) //
                .file(OUTPUT) //
                .count() //
                .get() //
                .intValue());
    }

    @Test
    public void searchSimpleUsingFileUrl() throws FileNotFoundException, IOException {
        Index<String> index = createSimpleIndex();
        Bounds queryBounds = SIMPLE_BOUNDS_WHOLE_DOMAIN;
        URL url = OUTPUT.toURI().toURL();
        // Note that Range request header will be ignored making a connection to a
        // file:// url so we read the whole file every time
        assertEquals(NUM_SIMPLE_ROWS, index.search(queryBounds).url(url.toString()).count().get().intValue());
    }

    private static Index<String> createSimpleIndex() throws IOException, FileNotFoundException {
        String s = "10,2,300\n4,5,600\n8,7,100";
        File input = new File("target/input");
        Files.write(input.toPath(), s.getBytes(StandardCharsets.UTF_8));

        int bits = 2;
        int dimensions = 3;
        int approxNumIndexEntries = 2;
        return Index //
                .builder() //
                .serializer(SIMPLE_SERIALIZER) //
                .pointMapper(SIMPLE_POINT_MAPPER) //
                .input(input) //
                .output(OUTPUT) //
                .bits(bits) //
                .dimensions(dimensions) //
                .numIndexEntries(approxNumIndexEntries) //
                .createIndex();
    }

    @Test
    public void testCalculationOfIndex() throws FileNotFoundException, IOException {
        Index<byte[]> index = createIndex();

        System.out.println(index);
        System.out.println(new Date(Math.round(index.mins()[2])));

        checkIndex(index);
    }

    @Test
    public void testIndexSerializationRoundTrip() throws IOException {
        Index<byte[]> index = createIndex();

        File idx = new File("target/output.idx");

        // check serialization
        index.write(idx);

        // reread index
        index = Index.serializer(SERIALIZER).pointMapper(POINT_FN).read(idx);
        checkIndex(index);

        index = Index.serializer(SERIALIZER).pointMapper(POINT_FN).read(idx);
        checkIndex(index);
    }

    @Test(expected = UncheckedIOException.class)
    public void testIndexReadFromEmptyFileThrows() {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(new byte[0]));
        Index.serializer(SIMPLE_SERIALIZER).pointMapper(SIMPLE_POINT_MAPPER).read(dis);
    }

    @Test
    public void testClose() {
        final boolean[] b = new boolean[1];
        Closeable c = new Closeable() {

            @Override
            public void close() throws IOException {
                b[0] = true;
            }
        };
        assertFalse(b[0]);
        Index.closeSilently(c);
        assertTrue(b[0]);
    }

    @Test
    public void testCloseThrowsIgnoredByCloseSilently() {
        Closeable c = new Closeable() {

            @Override
            public void close() throws IOException {
                throw new IOException("boo");
            }
        };
        Index.closeSilently(c);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTooManyBits() {
        int bits = 64;
        int dimensions = 3;
        File input = new File("src/test/resources/2019-05-15.binary-fixes-with-mmsi.sampled.every.400");
        int approximateNumIndexEntries = 100;
        Index //
                .serializer(SERIALIZER) //
                .pointMapper(POINT_FN) //
                .input(input) //
                .output(OUTPUT) //
                .bits(bits) //
                .dimensions(dimensions) //
                .numIndexEntries(approximateNumIndexEntries) //
                .createIndex();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testOrdinatesLength() throws FileNotFoundException, IOException {
        Index<String> index = createSimpleIndex();
        index.ordinates(new double[1]);
    }

    @Test
    public void testCloseNull() {
        Index.closeSilently(null);
    }

    @Test
    public void testGetRangeHeaderValueWithEnd() {
        // see rfc7233
        assertEquals("bytes=3-10", Index.getRangeHeaderValue(3L, Optional.of(10L)));
    }

    @Test
    public void testGetRangeHeaderValueWithoutEnd() {
        // see rfc7233
        assertEquals("bytes=3", Index.getRangeHeaderValue(3L, Optional.empty()));
    }

    @Test(expected = UncheckedException.class)
    public void testInputStreamFactoryThrows() throws FileNotFoundException, IOException {
        Index<String> idx = createSimpleIndex();
        idx //
                .search(new double[3], new double[3]) //
                .inputStreamFactory((x, y) -> {
                    throw new IOException("boo");
                }).count().get();
    }

    @Test(expected = UncheckedIOException.class)
    public void testSearchFileDoesNotExist() throws FileNotFoundException, IOException {
        Index<String> idx = createSimpleIndex();
        idx //
                .search(new double[3], new double[3]) //
                .file(new File("target/doesnotexist")) //
                .count().get();
    }

    @Test(expected = RuntimeException.class)
    public void testSearchMalformedUrl() throws FileNotFoundException, IOException {
        Index<String> idx = createSimpleIndex();
        idx //
                .search(new double[3], new double[3]) //
                .url("hithere") //
                .count().get();
    }

    @Test(expected = UncheckedIOException.class)
    public void testIndexWriteToDataOutputStreamThrows() throws FileNotFoundException, IOException {
        DataOutputStream dos = new DataOutputStream(new OutputStream() {

            @Override
            public void write(int b) throws IOException {
                throw new IOException("boo");
            }
        });
        createSimpleIndex().write(dos);
    }

    @Test
    public void testQuery() throws IOException {
        Index<byte[]> ind = createIndex();
        Bounds sb = createQueryBounds(Math.round(ind.mins()[2]), Math.round(ind.maxes()[2]));
        int expectedFound = 0;
        {
            Reader<byte[]> r = SERIALIZER.createReader(Util.bufferedInput(OUTPUT));
            byte[] b;
            while ((b = r.read()) != null) {
                Record rec = Record.read(b);
                if (sb.contains(rec.toArray())) {
                    expectedFound++;
                }
            }
        }

        long[] o1 = ind.ordinates(sb.mins());
        long[] o2 = ind.ordinates(sb.maxes());
        Ranges ranges = ind.hilbertCurve().query(o1, o2);
        List<PositionRange> positionRanges = ind.positionRanges(ranges);
        try (RandomAccessFile raf = new RandomAccessFile(OUTPUT, "r")) {
            List<Record> list = Stream //
                    .from(positionRanges) //
                    .flatMap(pr -> ind.search(sb, raf, pr)) //
                    .map(b -> Record.read(b)) //
                    .toList() //
                    .get();
            assertEquals(expectedFound, list.size());
        }
    }

    private static Index<byte[]> createIndex() throws IOException {
        int bits = 10;
        int dimensions = 3;
        File input = new File("src/test/resources/2019-05-15.binary-fixes-with-mmsi.sampled.every.400");
        int approximateNumIndexEntries = 100;
        Index<byte[]> index = Index //
                .serializer(SERIALIZER) //
                .pointMapper(POINT_FN) //
                .input(input) //
                .output(OUTPUT) //
                .bits(bits) //
                .dimensions(dimensions) //
                .numIndexEntries(approximateNumIndexEntries) //
                .sortMaxFilesPerMerge(10000) //
                .sortMaxItemsPerFile(100000) //
                .createIndex("target/created-index");
        assertTrue(new File("target/created-index").exists());
        return index;
    }

    private void checkIndex(Index<?> index) {
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
        return Bounds.create(new double[] { lat1, lon1, t1 }, new double[] { lat2, lon2, t2 });
    }

}
