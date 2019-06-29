package com.github.davidmoten.shi;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;

import org.davidmoten.hilbert.HilbertCurve;
import org.davidmoten.hilbert.Range;
import org.davidmoten.hilbert.Ranges;
import org.davidmoten.hilbert.SmallHilbertCurve;
import org.davidmoten.kool.Stream;
import org.davidmoten.kool.function.BiFunction;

import com.github.davidmoten.bigsorter.Reader;
import com.github.davidmoten.bigsorter.Serializer;
import com.github.davidmoten.bigsorter.Sorter;
import com.github.davidmoten.bigsorter.Writer;
import com.github.davidmoten.guavamini.Preconditions;
import com.github.davidmoten.guavamini.annotations.VisibleForTesting;

import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;

public final class Index<T> {

    private static final short VERSION = 1;
    private final TreeMap<Integer, Long> indexPositions;
    private final double[] mins;
    private final double[] maxes;
    private final SmallHilbertCurve hc;
    private final long count;
    private final Serializer<? extends T> serializer;
    private final Function<? super T, double[]> pointMapper;

    Index(TreeMap<Integer, Long> indexPositions, double[] mins, double[] maxes, int bits,
            long count, Serializer<? extends T> serializer,
            Function<? super T, double[]> pointMapper) {
        this.indexPositions = indexPositions;
        this.mins = mins;
        this.maxes = maxes;
        this.count = count;
        this.serializer = serializer;
        this.pointMapper = pointMapper;
        this.hc = HilbertCurve.small().bits(bits).dimensions(mins.length);
    }

    public Serializer<? extends T> serializer() {
        return serializer;
    }

    public Function<? super T, double[]> pointMapper() {
        return pointMapper;
    }

    public static <T> Builder1<T> serializer(Serializer<? extends T> serializer) {
        return new Builder1<T>(serializer);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        Builder() {
            // prevent instantiation externally
        }

        public <T> Builder1<T> serializer(Serializer<? extends T> serializer) {
            return new Builder1<T>(serializer);
        }
    }

    public static final class Builder1<T> {
        private final Serializer<? extends T> serializer;
        Function<? super T, double[]> pointMapper;
        File input;
        File output;
        int bits;
        int dimensions;
        int numIndexEntriesApproximate = 10000;
        int sortMaxFilesPerMerge = 100;
        int sortMaxItemsPerFile = 100000;

        Builder1(Serializer<? extends T> serializer) {
            this.serializer = serializer;
        }

        public Builder2<T> pointMapper(Function<? super T, double[]> pointMapper) {
            this.pointMapper = pointMapper;
            return new Builder2<T>(this);
        }
    }

    public static final class Builder2<T> {

        private final Builder1<T> b;

        Builder2(Builder1<T> b) {
            this.b = b;
        }

        public Builder3<T> input(File input) {
            b.input = input;
            return new Builder3<T>(b);
        }

        public Builder3<T> input(String filename) {
            return input(new File(filename));
        }

        public Index<T> read(DataInputStream in) {
            return Index.read(in, b.serializer, b.pointMapper);
        }

        public Index<T> read(File file) {
            try (DataInputStream in = new DataInputStream(
                    new BufferedInputStream(new FileInputStream(file)))) {
                return read(in);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        public Index<T> read(URL url) {
            try (DataInputStream in = new DataInputStream(
                    new BufferedInputStream(url.openStream()))) {
                return read(in);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public static final class Builder3<T> {
        private final Builder1<T> b;

        Builder3(Builder1<T> b) {
            this.b = b;
        }

        public Builder4<T> output(File output) {
            b.output = output;
            return new Builder4<T>(b);
        }

        public Builder4<T> output(String output) {
            return output(new File(output));
        }
    }

    public static final class Builder4<T> {

        private final Builder1<T> b;

        Builder4(Builder1<T> b) {
            this.b = b;
        }

        public Builder5<T> bits(int bits) {
            b.bits = bits;
            return new Builder5<T>(b);
        }

    }

    public static final class Builder5<T> {

        private final Builder1<T> b;

        Builder5(Builder1<T> b) {
            this.b = b;
        }

        public Builder6<T> dimensions(int dimensions) {
            b.dimensions = dimensions;
            return new Builder6<T>(b);
        }
    }

    public static final class Builder6<T> {

        private final Builder1<T> b;

        Builder6(Builder1<T> b) {
            this.b = b;
        }

        /**
         * Sets the <i>approximate</i> number of index entries. The number required will
         * depend on where the chunking falls so can vary by a few from the desired
         * value.
         * 
         * @param numIndexEntries approximate number of index entries
         * @return builder
         */
        public Builder6<T> numIndexEntries(int numIndexEntries) {
            b.numIndexEntriesApproximate = numIndexEntries;
            return this;
        }

        public Builder6<T> sortMaxFilesPerMerge(int sortMaxFilesPerMerge) {
            b.sortMaxFilesPerMerge = sortMaxFilesPerMerge;
            return this;
        }

        public Builder6<T> sortMaxItemsPerFile(int sortMaxItemsPerFile) {
            b.sortMaxItemsPerFile = sortMaxItemsPerFile;
            return this;
        }

        public Index<T> createIndex(String filename) {
            return createIndex(new File(filename));
        }

        public Index<T> createIndex(File file) {
            return createIndex().write(file);
        }

        public Index<T> createIndex() {
            try {
                return Index.createIndex(b.input, b.serializer, b.pointMapper, b.output, b.bits,
                        b.dimensions, b.numIndexEntriesApproximate, b.sortMaxFilesPerMerge,
                        b.sortMaxItemsPerFile);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @VisibleForTesting
    TreeMap<Integer, Long> indexPositions() {
        return indexPositions;
    }

    /**
     * Fits the desired ranges to the effective querying ranges according to the
     * known index positions.
     * 
     * @param ranges list of ranges in ascending order
     * @return querying ranges based on known index positions
     */
    public List<PositionRange> positionRanges(Iterable<Range> ranges) {
        return positionRanges(indexPositions, ranges);
    }

    @VisibleForTesting
    static List<PositionRange> positionRanges(TreeMap<Integer, Long> indexPositions,
            Iterable<Range> ranges) {
        LinkedList<PositionRange> list = new LinkedList<>();
        for (Range range : ranges) {
            if (range.low() <= indexPositions.lastKey()
                    && range.high() >= indexPositions.firstKey()) {
                Long startPosition = value(indexPositions.floorEntry((int) range.low()));
                if (startPosition == null) {
                    startPosition = indexPositions.firstEntry().getValue();
                }
                Long endPosition = value(indexPositions.higherEntry((int) range.high()));
                if (endPosition == null) {
                    endPosition = Long.MAX_VALUE;
                }
                PositionRange p = new PositionRange(range.high(), startPosition, endPosition);
                append(list, p);
            }
        }
        return list;
    }

    private static void append(LinkedList<PositionRange> list, PositionRange p) {
        if (list.isEmpty()) {
            list.offer(p);
        } else {
            PositionRange last = list.getLast();
            if (p.floorPosition() <= last.ceilingPosition()) {
                list.pollLast();
                list.offer(last.join(p));
            } else {
                list.offer(p);
            }
        }
    }

    private static <T, R> R value(Entry<T, R> entry) {
        if (entry == null) {
            return null;
        } else {
            return entry.getValue();
        }
    }

    public double[] mins() {
        return mins;
    }

    public double[] maxes() {
        return maxes;
    }

    /**
     * Returns count of records in file indexed by this.
     * 
     * @return count of records in file indexed by this.
     */
    public long count() {
        return count;
    }

    public long[] ordinates(double... d) {
        Preconditions.checkArgument(d.length == mins.length);
        long[] x = new long[d.length];
        for (int i = 0; i < d.length; i++) {
            if (mins[i] == maxes[i]) {
                x[i] = 0;
            } else {
                x[i] = Math.round(((Math.min(d[i], maxes[i]) - mins[i]) / (maxes[i] - mins[i]))
                        * hc.maxOrdinate());
            }
        }
        return x;
    }

    public SmallHilbertCurve hilbertCurve() {
        return hc;
    }

    private static <T> Index<T> read(DataInputStream dis, Serializer<? extends T> serializer,
            Function<? super T, double[]> point) {
        try {
            // read version
            dis.readShort();
            int bits = dis.readInt();
            int dimensions = dis.readInt();
            double[] mins = new double[dimensions];
            double[] maxes = new double[dimensions];
            for (int i = 0; i < dimensions; i++) {
                mins[i] = dis.readDouble();
                maxes[i] = dis.readDouble();
            }
            long count = dis.readLong();
            int numEntries = dis.readInt();
            boolean useLongPositions = dis.readInt() == 1;

            TreeMap<Integer, Long> indexPositions = new TreeMap<Integer, Long>();
            for (int i = 0; i < numEntries; i++) {
                int index = dis.readInt();
                final long pos;
                if (useLongPositions) {
                    pos = dis.readLong();
                } else {
                    pos = (long) dis.readInt();
                }
                indexPositions.put(index, pos);
            }
            return new Index<T>(indexPositions, mins, maxes, bits, count, serializer, point);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Index<T> write(File idx) {
        try (DataOutputStream dos = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream(idx)))) {
            write(dos);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return this;
    }

    public Index<T> write(DataOutputStream dos) throws IOException {
        try {
            dos.writeShort(VERSION);
            dos.writeInt(hc.bits());
            dos.writeInt(hc.dimensions());
            for (int i = 0; i < hc.dimensions(); i++) {
                dos.writeDouble(mins[i]);
                dos.writeDouble(maxes[i]);
            }

            dos.writeLong(count);

            // num index entries
            dos.writeInt(indexPositions.size());

            boolean useLongPositions = Stream.from(indexPositions.values())
                    .findFirst(x -> x > Integer.MAX_VALUE).get().isPresent();

            // write 0 for int position
            // write 1 for long position
            dos.writeInt(useLongPositions ? 1 : 0);

            for (Entry<Integer, Long> entry : indexPositions.entrySet()) {
                dos.writeInt(entry.getKey());
                long pos = entry.getValue();
                if (useLongPositions) {
                    dos.writeLong(pos);
                } else {
                    dos.writeInt((int) pos);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return this;
    }

    private static BiFunction<Long, Optional<Long>, InputStream> rafInputStreamFactory(File file) {
        return (first, last) -> {
            RandomAccessFile raf = createRaf(file);
            raf.seek(first);
            return new ClosingInputStream( //
                    new LimitingInputStream( //
                            new BufferedInputStream(Channels.newInputStream(raf.getChannel())),
                            last.orElse(Long.MAX_VALUE) - first),
                    () -> raf.close());
        };
    }

    @VisibleForTesting
    Flowable<T> search(Bounds queryBounds, File file, PositionRange pr) throws IOException {
        return search(queryBounds, rafInputStreamFactory(file), pr);
    }

    @VisibleForTesting
    Flowable<T> search(Bounds queryBounds, BiFunction<Long, Optional<Long>, InputStream> factory,
            PositionRange pr) throws IOException {
        return Flowable.defer(() -> {
            return getValues(factory, pr) //
                    .takeUntil(rec -> hc.index(ordinates(pointMapper.apply(rec))) > pr
                            .maxHilbertIndex()) //
                    .filter(t -> queryBounds.contains(pointMapper.apply(t)));
        });
    }

    static final class Counts {
        final long startTime;
        long recordsRead;
        long recordsFound;
        long positionRanges;
        long bytesRead;
        long totalTimeToFirstByte;

        Counts() {
            this.startTime = System.currentTimeMillis();
        }

        synchronized void incrementRecordsRead() {
            recordsRead++;
        }

        synchronized void incrementRecordsFoundAndAddTTFBAndAddBytesRead(long ttfb, long bytes) {
            recordsFound++;
            totalTimeToFirstByte += ttfb;
            bytesRead += bytes;
        }

    }

    @VisibleForTesting
    Flowable<WithStats<T>> searchWithStats(Bounds queryBounds,
            BiFunction<Long, Optional<Long>, InputStream> factory, PositionRange pr, Counts counts)
            throws IOException {
        counts.positionRanges++;
        CountingInputStream[] in = new CountingInputStream[1];
        BiFunction<Long, Optional<Long>, InputStream> factoryWithCount = (x, y) -> {
            long startTime = System.currentTimeMillis();
            CountingInputStream is = new CountingInputStream(factory.apply(x, y), startTime);
            in[0] = is;
            return is;
        };
        return getValues(factoryWithCount, pr) //
                .doOnNext(x -> counts.incrementRecordsRead()) //
                .takeUntil(
                        rec -> hc.index(ordinates(pointMapper.apply(rec))) > pr.maxHilbertIndex()) //
                .filter(t -> queryBounds.contains(pointMapper.apply(t))) //
                .doOnNext(x -> counts.incrementRecordsFoundAndAddTTFBAndAddBytesRead(
                        in[0].readTimeToFirstByteAndSetToZero(), in[0].count())) //
                .map(x -> {
                    synchronized (counts) {
                        return new WithStats<T>(x, counts.recordsRead, counts.recordsFound,
                                counts.bytesRead, counts.totalTimeToFirstByte,
                                counts.positionRanges,
                                System.currentTimeMillis() - counts.startTime);
                    }
                });
    }

    private Flowable<T> getValues(BiFunction<Long, Optional<Long>, InputStream> factory,
            PositionRange pr) {
        return Flowable.defer(() -> {
            InputStream[] in = new InputStream[1];
            final Reader<? extends T> r;
            try {
                Optional<Long> ceiling = pr.ceilingPosition() == Long.MAX_VALUE ? Optional.empty()
                        : Optional.of(pr.ceilingPosition());
                // TODO don't block
                in[0] = factory.apply(pr.floorPosition(), ceiling);
                r = serializer.createReader(in[0]);
            } catch (Throwable t) {
                closeSilently(in[0]);
                return Flowable.error(t);
            }
            return Flowable.<T>generate( //
                    emitter -> {
                        T t;
                        while (true) {
                            t = r.read();
                            if (t == null) {
                                emitter.onComplete();
                                break;
                            } else {
                                emitter.onNext(t);
                                break;
                            }
                            // else keep reading till EOF or next record found within queryBounds
                        }
                    }) //
                    .doOnCancel(() -> {
                        closeSilently(r);
                        closeSilently(in[0]);
                    });
        });
    }

    @VisibleForTesting
    static void closeSilently(Closeable c) {
        try {
            if (c != null) {
                c.close();
            }
        } catch (Throwable t) {
            // do nothing
        }
    }

    public int numEntries() {
        return indexPositions.size();
    }

    public SearchBuilder search(double[] a, double[] b) {
        return search(Bounds.create(a, b));
    }

    public SearchBuilder search(Bounds bounds) {
        return new SearchBuilder(bounds);
    }

    public final class SearchBuilder {

        private final Bounds bounds;
        private int maxRanges;
        private int rangesBufferSize;
        private int concurrency = 1;

        SearchBuilder(Bounds bounds) {
            this.bounds = bounds;
        }

        public SearchBuilderWithStats withStats() {
            return new SearchBuilderWithStats(this);
        }

        public SearchBuilderAdvanced advanced() {
            return new SearchBuilderAdvanced(this);
        }

        public SearchBuilder maxRanges(int maxRanges) {
            this.maxRanges = maxRanges;
            return this;
        }

        public SearchBuilder rangesBufferSize(int rangeBufferSize) {
            this.rangesBufferSize = rangeBufferSize;
            return this;
        }

        public SearchBuilder concurrency(int concurrency) {
            Preconditions.checkArgument(concurrency > 0, "concurrency must be greater than zero");
            this.concurrency = concurrency;
            return this;
        }

        public Flowable<T> file(File file) {
            return Flowable.defer(() -> inputStreamFactory(rafInputStreamFactory(file)));
        }

        public Flowable<T> file(String filename) {
            return file(new File(filename));
        }

        public Flowable<T> inputStreamFactory(
                BiFunction<Long, Optional<Long>, InputStream> inputStreamFactory) {
            if (concurrency == 1) {
                return search(bounds, inputStreamFactory, maxRanges, rangesBufferSize);
            } else {
                return advanced() //
                        .inputStreamFactory(inputStreamFactory) //
                        .flatMap(x -> x.subscribeOn(Schedulers.io()), concurrency);
            }
        }

        public Flowable<T> url(String url) {
            try {
                return url(new URL(url));
            } catch (MalformedURLException e) {
                throw new RuntimeException(e);
            }
        }

        public Flowable<T> url(URL url) {
            return inputStreamFactory(inputStreamForRange(url));
        }
    }

    public final class SearchBuilderAdvanced {

        private final Index<T>.SearchBuilder b;

        SearchBuilderAdvanced(SearchBuilder b) {
            this.b = b;
        }

        public SearchBuilderWithStats withStats() {
            return new SearchBuilderWithStats(b);
        }

        public SearchBuilderAdvanced maxRanges(int maxRanges) {
            b.maxRanges = maxRanges;
            return this;
        }

        public SearchBuilderAdvanced rangesBufferSize(int rangeBufferSize) {
            b.rangesBufferSize = rangeBufferSize;
            return this;
        }

        public Flowable<Flowable<T>> file(File file) {
            return Flowable.defer(() -> inputStreamFactory(rafInputStreamFactory(file)));
        }

        public Flowable<Flowable<T>> file(String filename) {
            return file(new File(filename));
        }

        public Flowable<Flowable<T>> inputStreamFactory(
                BiFunction<Long, Optional<Long>, InputStream> inputStreamFactory) {
            return searchAdvanced(b.bounds, inputStreamFactory, b.maxRanges, b.rangesBufferSize);
        }

        public Flowable<Flowable<T>> url(String url) {
            try {
                return url(new URL(url));
            } catch (MalformedURLException e) {
                throw new RuntimeException(e);
            }
        }

        public Flowable<Flowable<T>> url(URL url) {
            return inputStreamFactory(inputStreamForRange(url));
        }

    }

    public final class SearchBuilderWithStats {

        private final Index<T>.SearchBuilder b;

        SearchBuilderWithStats(Index<T>.SearchBuilder b) {
            this.b = b;
        }

        public SearchBuilderWithStats maxRanges(int maxRanges) {
            b.maxRanges = maxRanges;
            return this;
        }

        public SearchBuilderWithStats rangesBufferSize(int rangeBufferSize) {
            b.rangesBufferSize = rangeBufferSize;
            return this;
        }

        public SearchBuilderWithStats concurrency(int concurrency) {
            b.concurrency(concurrency);
            return this;
        }

        public SearchBuilderWithStatsAdvanced advanced() {
            return new SearchBuilderWithStatsAdvanced(b);
        }

        public Flowable<WithStats<T>> file(File file) {
            return Flowable.defer(() -> inputStreamFactory(rafInputStreamFactory(file)));
        }

        public Flowable<WithStats<T>> inputStreamFactory(
                BiFunction<Long, Optional<Long>, InputStream> inputStreamFactory) {
            if (b.concurrency == 1) {
                return searchWithStats(b.bounds, inputStreamFactory, b.maxRanges,
                        b.rangesBufferSize);
            } else {
                return advanced() //
                        .inputStreamFactory(inputStreamFactory) //
                        .flatMap(x -> x.subscribeOn(Schedulers.io()), b.concurrency);
            }
        }

        public Flowable<WithStats<T>> url(String url) {
            try {
                return url(new URL(url));
            } catch (MalformedURLException e) {
                throw new RuntimeException(e);
            }
        }

        public Flowable<WithStats<T>> url(URL url) {
            return inputStreamFactory(inputStreamForRange(url));
        }

    }

    public final class SearchBuilderWithStatsAdvanced {

        private final Index<T>.SearchBuilder b;

        SearchBuilderWithStatsAdvanced(SearchBuilder b) {
            this.b = b;
        }

        public SearchBuilderWithStats withStats() {
            return new SearchBuilderWithStats(b);
        }

        public SearchBuilderWithStatsAdvanced maxRanges(int maxRanges) {
            b.maxRanges = maxRanges;
            return this;
        }

        public SearchBuilderWithStatsAdvanced rangesBufferSize(int rangeBufferSize) {
            b.rangesBufferSize = rangeBufferSize;
            return this;
        }

        public Flowable<Flowable<WithStats<T>>> file(File file) {
            return Flowable.defer(() -> inputStreamFactory(rafInputStreamFactory(file)));
        }

        public Flowable<Flowable<WithStats<T>>> file(String filename) {
            return file(new File(filename));
        }

        public Flowable<Flowable<WithStats<T>>> inputStreamFactory(
                BiFunction<Long, Optional<Long>, InputStream> inputStreamFactory) {
            return searchWithStatsAdvanced(b.bounds, inputStreamFactory, b.maxRanges,
                    b.rangesBufferSize);
        }

        public Flowable<Flowable<WithStats<T>>> url(String url) {
            try {
                return url(new URL(url));
            } catch (MalformedURLException e) {
                throw new RuntimeException(e);
            }
        }

        public Flowable<Flowable<WithStats<T>>> url(URL url) {
            return inputStreamFactory(inputStreamForRange(url));
        }

    }

    private static RandomAccessFile createRaf(File f) {
        try {
            return new RandomAccessFile(f, "r");
        } catch (FileNotFoundException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Flowable<T> search(Bounds queryBounds,
            BiFunction<Long, Optional<Long>, InputStream> inputStreamFactory, int maxRanges,
            int rangesBufferSize) {
        return Flowable.defer(() -> {
            long[] a = ordinates(queryBounds.mins());
            long[] b = ordinates(queryBounds.maxes());
            Ranges ranges = hc.query(a, b, maxRanges, rangesBufferSize);
            return Flowable.fromIterable(positionRanges(ranges)) //
                    .flatMap(pr -> search(queryBounds, inputStreamFactory, pr));
        });
    }

    private Flowable<Flowable<T>> searchAdvanced(Bounds queryBounds,
            BiFunction<Long, Optional<Long>, InputStream> inputStreamFactory, int maxRanges,
            int rangesBufferSize) {
        return Flowable.defer(() -> {
            long[] a = ordinates(queryBounds.mins());
            long[] b = ordinates(queryBounds.maxes());
            // TODO make hc.query return a Flowable (lazy calculation)?
            Ranges ranges = hc.query(a, b, maxRanges, rangesBufferSize);
            return Flowable.fromIterable(positionRanges(ranges)) //
                    .map(pr -> search(queryBounds, inputStreamFactory, pr));
        });
    }

    private Flowable<Flowable<WithStats<T>>> searchWithStatsAdvanced(Bounds queryBounds,
            BiFunction<Long, Optional<Long>, InputStream> inputStreamFactory, int maxRanges,
            int rangesBufferSize) {
        return Flowable.defer(() -> {
            long[] a = ordinates(queryBounds.mins());
            long[] b = ordinates(queryBounds.maxes());
            Ranges ranges = hc.query(a, b, maxRanges, rangesBufferSize);
            Counts counts = new Counts();
            return Flowable.fromIterable(positionRanges(ranges)) //
                    .map(pr -> searchWithStats(queryBounds, inputStreamFactory, pr, counts)) //
                    .concatWith(Flowable.just(finalStats(counts)));
        });
    }

    private Flowable<WithStats<T>> searchWithStats(Bounds queryBounds,
            BiFunction<Long, Optional<Long>, InputStream> inputStreamFactory, int maxRanges,
            int rangesBufferSize) {
        return Flowable.defer(() -> {
            long[] a = ordinates(queryBounds.mins());
            long[] b = ordinates(queryBounds.maxes());
            Ranges ranges = hc.query(a, b, maxRanges, rangesBufferSize);
            Counts counts = new Counts();
            return Flowable.fromIterable(positionRanges(ranges)) //
                    .flatMap(pr -> searchWithStats(queryBounds, inputStreamFactory, pr, counts))
                    .concatWith(finalStats(counts));
        });
    }

    private Flowable<WithStats<T>> finalStats(Counts counts) {
        return Flowable.defer(() -> {
            synchronized (counts) {
                return Flowable.just(new WithStats<T>(null, counts.recordsRead, counts.recordsFound,
                        counts.bytesRead, counts.totalTimeToFirstByte, counts.positionRanges,
                        System.currentTimeMillis() - counts.startTime));
            }
        });
    }

    private static BiFunction<Long, Optional<Long>, InputStream> inputStreamForRange(URL u) {
        return (start, end) -> {
            URLConnection con = u.openConnection();
            String bytesRange = getRangeHeaderValue(start, end);
            con.addRequestProperty("Range", bytesRange);
            return new BufferedInputStream(con.getInputStream());
        };
    }

    @VisibleForTesting
    static String getRangeHeaderValue(long start, Optional<Long> end) {
        return "bytes=" + start + end.map(x -> "-" + x).orElse("");
    }

    private static <T> Index<T> createIndex( //
            File input, //
            Serializer<? extends T> serializer, //
            Function<? super T, double[]> point, //
            File output, //
            int bits, //
            int dimensions, //
            int numIndexEntriesApproximate, //
            int sortMaxFiles, //
            int sortMaxItemsPerFile) //
            throws IOException {

        Preconditions.checkArgument(bits * dimensions <= 31,
                "bits * dimensions must be at most 31");

        // scan once to get the mins, maxes, count
        final double[] mins = new double[dimensions];
        final double[] maxes = new double[dimensions];
        long count = 0;
        try (InputStream in = Util.bufferedInput(input); //
                Reader<? extends T> reader = serializer.createReader(in)) {
            Arrays.setAll(mins, i -> Double.MAX_VALUE);
            Arrays.setAll(maxes, i -> Double.MIN_VALUE);
            T t;
            while ((t = reader.read()) != null) {
                count++;
                double[] p = point.apply(t);
                if (p.length != dimensions) {
                    throw new IllegalArgumentException(
                            "point function should be of length equal to number of dimensions but was: "
                                    + Arrays.toString(p));
                }
                for (int i = 0; i < p.length; i++) {
                    if (p[i] < mins[i]) {
                        mins[i] = p[i];
                    }
                    if (p[i] > maxes[i]) {
                        maxes[i] = p[i];
                    }
                }
            }
        }

        SmallHilbertCurve hc = HilbertCurve.small().bits(bits).dimensions(dimensions);

        Sorter //
                .serializer(serializer) //
                .comparator((a, b) -> {
                    double[] x = point.apply(a);
                    double[] y = point.apply(b);
                    return Integer.compare( //
                            hilbertIndex(hc, x, mins, maxes), //
                            hilbertIndex(hc, y, mins, maxes));
                }) //
                .input(input) //
                .output(output) //
                .maxFilesPerMerge(sortMaxFiles) //
                .maxItemsPerFile(sortMaxItemsPerFile) //
                .loggerStdOut() //
                .sort();

        long chunk = Math.max(1, count / numIndexEntriesApproximate);
        TreeMap<Integer, Long> indexPositions = createIndexPositions(serializer, point, output,
                mins, maxes, hc, chunk);
        return new Index<T>(indexPositions, mins, maxes, bits, count, serializer, point);
    }

    private static <T> TreeMap<Integer, Long> createIndexPositions(Serializer<T> serializer,
            Function<? super T, double[]> point, File output, final double[] mins,
            final double[] maxes, SmallHilbertCurve hc, long chunk)
            throws IOException, FileNotFoundException {
        TreeMap<Integer, Long> indexPositions = new TreeMap<>();
        try (//
                InputStream in = Util.bufferedInput(output); //
                Reader<T> reader = serializer.createReader(in);
                CountingOutputStream counter = new CountingOutputStream();
                Writer<T> writer = serializer.createWriter(counter)) {
            T t;
            long position = 0;
            T lastT = null;
            while ((t = reader.read()) != null) {
                position = counter.count();
                if (position % chunk == 0) {
                    double[] p = point.apply(t);
                    int index = hilbertIndex(hc, p, mins, maxes);
                    if (!indexPositions.containsKey(index)) {
                        // don't overwrite an earlier start position for the index
                        indexPositions.put(index, position);
                    }
                }
                writer.write(t);

                // must flush otherwise position may be wrong for the next pass through the loop
                writer.flush();

                lastT = t;
            }
            if (counter.count() % chunk != 0) {
                // write the last record too so we know index of last position
                double[] p = point.apply(lastT);
                int index = hilbertIndex(hc, p, mins, maxes);
                indexPositions.put(index, position);
            }
        }
        return indexPositions;
    }

    private static int hilbertIndex(SmallHilbertCurve hc, double[] point, double[] mins,
            double[] maxes) {
        long[] ordinates = new long[point.length];
        for (int i = 0; i < ordinates.length; i++) {
            ordinates[i] = Math
                    .round((point[i] - mins[i]) / (maxes[i] - mins[i]) * hc.maxOrdinate());
        }
        // can do this because bits * dimensions <= 31
        return (int) hc.index(ordinates);
    }

    @Override
    public String toString() {
        return "Index [mins=" + Arrays.toString(mins) + ", maxes=" + Arrays.toString(maxes)
                + ", numEntries=" + indexPositions.size() + "]";
    }

}
