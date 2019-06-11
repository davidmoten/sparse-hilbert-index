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
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
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
import org.davidmoten.kool.StreamIterable;
import org.davidmoten.kool.function.BiFunction;

import com.github.davidmoten.bigsorter.Reader;
import com.github.davidmoten.bigsorter.Serializer;
import com.github.davidmoten.guavamini.Preconditions;
import com.github.davidmoten.guavamini.annotations.VisibleForTesting;

public final class Index<T> {

    private static final short VERSION = 1;
    private final TreeMap<Integer, Long> indexPositions;
    private final double[] mins;
    private final double[] maxes;
    private final SmallHilbertCurve hc;
    private final long count;
    private final Serializer<? extends T> serializer;
    private final Function<? super T, double[]> pointMapper;

    Index(TreeMap<Integer, Long> indexPositions, double[] mins, double[] maxes, int bits, long count,
            Serializer<? extends T> serializer, Function<? super T, double[]> pointMapper) {
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

        public Index<T> read(DataInputStream in) {
            return Index.read(in, b.serializer, b.pointMapper);
        }

        public Index<T> read(File file) {
            try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))) {
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
         * @param numIndexEntries
         *            approximate number of index entries
         * @return builder
         */
        public Builder6<T> numIndexEntries(int numIndexEntries) {
            b.numIndexEntriesApproximate = numIndexEntries;
            return this;
        }

        public Index<T> createIndex() {
            try {
                return HilbertIndex.<T>createIndex(b.input, b.serializer, b.pointMapper, b.output, b.bits, b.dimensions,
                        b.numIndexEntriesApproximate);
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
     * @param ranges
     *            list of ranges in ascending order
     * @return querying ranges based on known index positions
     */
    public List<PositionRange> positionRanges(Iterable<Range> ranges) {
        return positionRanges(indexPositions, ranges);
    }

    @VisibleForTesting
    static List<PositionRange> positionRanges(TreeMap<Integer, Long> indexPositions, Iterable<Range> ranges) {
        LinkedList<PositionRange> list = new LinkedList<>();
        for (Range range : ranges) {
            if (range.low() <= indexPositions.lastKey() && range.high() >= indexPositions.firstKey()) {
                Long startPosition = value(indexPositions.floorEntry((int) range.low()));
                if (startPosition == null) {
                    startPosition = indexPositions.firstEntry().getValue();
                }
                Long endPosition = value(indexPositions.higherEntry((int) range.high()));
                if (endPosition == null) {
                    endPosition = Long.MAX_VALUE;
                }
                PositionRange p = new PositionRange(range.high(), startPosition, endPosition);
                if (list.isEmpty()) {
                    list.add(p);
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
        }
        return list;
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
            x[i] = Math.round(((Math.min(d[i], maxes[i]) - mins[i]) / (maxes[i] - mins[i])) * hc.maxOrdinate());
        }
        return x;
    }

    public SmallHilbertCurve hilbertCurve() {
        return hc;
    }

    public static <T> Index<T> read(File file, Serializer<? extends T> serializer, Function<? super T, double[]> point)
            throws FileNotFoundException, IOException {
        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))) {
            return read(dis, serializer, point);
        }
    }

    public static <T> Index<T> read(DataInputStream dis, Serializer<? extends T> serializer,
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
            dis.readInt();
            TreeMap<Integer, Long> indexPositions = new TreeMap<Integer, Long>();
            for (int i = 0; i < numEntries; i++) {
                int index = dis.readInt();
                int pos = dis.readInt();
                indexPositions.put(index, (long) pos);
            }
            return new Index<T>(indexPositions, mins, maxes, bits, count, serializer, point);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Index<T> write(File idx) {
        try (FileOutputStream fos = new FileOutputStream(idx)) {
            write(fos);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return this;
    }

    public Index<T> write(OutputStream out) throws IOException {
        try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(out))) {
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

            // write 0 for int position
            // write 1 for long position
            dos.writeInt(0);

            for (Entry<Integer, Long> entry : indexPositions.entrySet()) {
                dos.writeInt(entry.getKey());
                Long pos = entry.getValue();
                if (pos > Integer.MAX_VALUE) {
                    throw new RuntimeException("file size too big for integer positions in index entries");
                }
                dos.writeInt(entry.getValue().intValue());
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return this;
    }

    private static BiFunction<Long, Optional<Long>, InputStream> rafInputStreamFactory(RandomAccessFile raf) {
        return (first, last) -> {
            raf.seek(first);
            return new LimitingInputStream(new BufferedInputStream(Channels.newInputStream(raf.getChannel())),
                    last.orElse(Long.MAX_VALUE) - first);
        };
    }

    public StreamIterable<T> search(Bounds queryBounds, RandomAccessFile raf, PositionRange pr) throws IOException {
        return search(queryBounds, rafInputStreamFactory(raf), pr);
    }

    public StreamIterable<T> search(Bounds queryBounds, BiFunction<Long, Optional<Long>, InputStream> factory,
            PositionRange pr) throws IOException {
        InputStream[] in = new InputStream[1];
        final Reader<? extends T> r;
        try {
            Optional<Long> ceiling = pr.ceilingPosition() == Long.MAX_VALUE ? Optional.empty()
                    : Optional.of(pr.ceilingPosition());
            in[0] = factory.apply(pr.floorPosition(), ceiling);
            r = serializer.createReader(in[0]);
        } catch (Throwable t) {
            closeSilently(in[0]);
            return Stream.error(t);
        }
        return Stream.<T>generate(emitter -> {
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
                .takeUntil(rec -> hc.index(ordinates(pointMapper.apply(rec))) > pr.maxHilbertIndex()) //
                .filter(t -> queryBounds.contains(pointMapper.apply(t))) //
                .doOnDispose(() -> {
                    closeSilently(r);
                    closeSilently(in[0]);
                });
    }

    private static void closeSilently(Closeable c) {
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

    public Stream<T> search(Bounds queryBounds, File file) {
        return search(queryBounds, file, 0);
    }

    public Stream<T> search(Bounds queryBounds, File file, int maxRanges) {
        try {
            return search(queryBounds, new RandomAccessFile(file, "r"), maxRanges);
        } catch (FileNotFoundException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Stream<T> search(Bounds queryBounds, RandomAccessFile raf) {
        return search(queryBounds, raf, 0);
    }

    public Stream<T> search(Bounds queryBounds, RandomAccessFile raf, int maxRanges) {
        return search(queryBounds, rafInputStreamFactory(raf), 0);
    }

    public Stream<T> search(Bounds queryBounds, BiFunction<Long, Optional<Long>, InputStream> factory) {
        return search(queryBounds, factory, 0);
    }

    public Stream<T> search(Bounds queryBounds, BiFunction<Long, Optional<Long>, InputStream> factory, int maxRanges) {
        long[] a = ordinates(queryBounds.mins());
        long[] b = ordinates(queryBounds.maxes());
        Ranges ranges = hc.query(a, b, maxRanges);
        return Stream.from(positionRanges(ranges)) //
                .flatMap(pr -> search(queryBounds, factory, pr));
    }
    
    public Stream<T> search(Bounds queryBounds, URL url, int maxRanges) {
        return search(queryBounds, inputStreamForRange(url), maxRanges);
    }
    
    public Stream<T> search(Bounds queryBounds, URL url) {
        return search(queryBounds, url, 0);
    }
    
    private static BiFunction<Long, Optional<Long>, InputStream> inputStreamForRange(URL u) {
        BiFunction<Long, Optional<Long>, InputStream> factory = (start, end) -> {
            URLConnection con = u.openConnection();
            String bytesRange = "bytes=" + start + end.map(x -> "-" + x).orElse("");
            con.addRequestProperty("Range", bytesRange);
            return new BufferedInputStream(con.getInputStream());
        };
        return factory;
    }

    @Override
    public String toString() {
        return "Index [mins=" + Arrays.toString(mins) + ", maxes=" + Arrays.toString(maxes) + ", numEntries="
                + indexPositions.size() + "]";
    }

}
