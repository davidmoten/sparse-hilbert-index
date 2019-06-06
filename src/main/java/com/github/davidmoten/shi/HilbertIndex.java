package com.github.davidmoten.shi;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.function.Function;

import org.davidmoten.hilbert.HilbertCurve;
import org.davidmoten.hilbert.SmallHilbertCurve;

import com.github.davidmoten.bigsorter.Reader;
import com.github.davidmoten.bigsorter.Serializer;
import com.github.davidmoten.bigsorter.Sorter;
import com.github.davidmoten.guavamini.Preconditions;

public final class HilbertIndex {

    public static <T> void sortAndCreateIndex( //
            File input, //
            Serializer<T> serializer, //
            Function<? super T, double[]> point, //
            File output, //
            File idx, int bits, int dimensions) throws FileNotFoundException, IOException {
        Preconditions.checkArgument(bits * dimensions <= 31, "bits * dimensions must be at most 31");

        // scan once to get the mins, maxes, count
        final double[] mins = new double[dimensions];
        final double[] maxes = new double[dimensions];
        long count = 0;
        try (//
                InputStream in = Util.bufferedInput(input); //
                Reader<T> reader = serializer.createReader(in)) {
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

        // TODO create Index
    }

    private static int hilbertIndex(SmallHilbertCurve hc, double[] point, double[] mins, double[] maxes) {
        long[] ordinates = new long[point.length];
        for (int i = 0; i < ordinates.length; i++) {
            ordinates[i] = Math.round((point[i] - mins[i]) / (maxes[i] - mins[i]) * hc.maxOrdinate());
        }
        // can do this because bits * dimensions <= 31
        return (int) hc.index(ordinates);
    }

}
