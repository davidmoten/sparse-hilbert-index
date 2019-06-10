package com.github.davidmoten.shi;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.TreeMap;
import java.util.function.Function;

import org.davidmoten.hilbert.HilbertCurve;
import org.davidmoten.hilbert.SmallHilbertCurve;

import com.github.davidmoten.bigsorter.Reader;
import com.github.davidmoten.bigsorter.Serializer;
import com.github.davidmoten.bigsorter.Sorter;
import com.github.davidmoten.bigsorter.Writer;
import com.github.davidmoten.guavamini.Preconditions;

public final class HilbertIndex {

    public static <T> Builder1<T> serializer(Serializer<? extends T> serializer) {
        return new Builder1<T>(serializer);
    }

    public static final class Builder1<T> {
        private File input;
        public File output;
        public int bits;
        public int dimensions;
        public int numIndexEntriesApproximate;
        public Function<? super T, double[]> point;
        private Serializer<? extends T> serializer;

        Builder1(Serializer<? extends T> serializer) {
            this.serializer = serializer;
        }

        Builder2<T> point(Function<? super T, double[]> point) {
            this.point = point;
            return new Builder2<T>(this);
        }
    }

    public static final class Builder2<T> {

        private final Builder1<T> b;

        Builder2(Builder1<T> b) {
            this.b = b;
        }

        Builder3<T> input(File input) {
            b.input = input;
            return new Builder3<T>(b);
        }
    }

    public static final class Builder3<T> {
        private final Builder1<T> b;

        Builder3(Builder1<T> b) {
            this.b = b;
        }

        Builder4<T> output(File output) {
            b.output = output;
            return new Builder4<T>(b);
        }
    }

    public static final class Builder4<T> {

        private final Builder1<T> b;

        Builder4(Builder1<T> b) {
            this.b = b;
        }

        Builder5<T> bits(int bits) {
            b.bits = bits;
            return new Builder5<T>(b);
        }

    }

    public static final class Builder5<T> {

        private final Builder1<T> b;

        Builder5(Builder1<T> b) {
            this.b = b;
        }

        Builder6<T> dimensions(int dimensions) {
            b.dimensions = dimensions;
            return new Builder6<T>(b);
        }
    }

    public static final class Builder6<T> {

        private final Builder1<T> b;

        Builder6(Builder1<T> b) {
            this.b = b;
        }

        Builder6<T> numIndexEntriesApproximate(int value) {
            b.numIndexEntriesApproximate = value;
            return this;
        }

        Index<T> createIndex() {
            try {
                return HilbertIndex.<T>createIndex(b.input, b.serializer, b.point, b.output, b.bits,
                        b.dimensions, b.numIndexEntriesApproximate);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private static <T> Index<T> createIndex( //
            File input, //
            Serializer<? extends T> serializer, //
            Function<? super T, double[]> point, //
            File output, //
            int bits, //
            int dimensions, //
            int approximateNumIndexEntries) //
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
                .loggerStdOut() //
                .sort();

        long chunk = Math.max(1, count / approximateNumIndexEntries);
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
                    indexPositions.put(index, position);
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

}
