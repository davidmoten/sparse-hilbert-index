package com.github.davidmoten.shi.fixes;

import static com.github.davidmoten.shi.fixes.Fixes.idx;
import static com.github.davidmoten.shi.fixes.Fixes.output;
import static com.github.davidmoten.shi.fixes.Fixes.point;
import static com.github.davidmoten.shi.fixes.Fixes.ser;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

import com.github.davidmoten.bigsorter.Reader;
import com.github.davidmoten.shi.Bounds;
import com.github.davidmoten.shi.Index;

public class FixesSearch {

    public static void main(String[] args) throws FileNotFoundException, IOException {
        Index<byte[]> index = Index.serializer(ser).pointMapper(point).read(idx);
        System.out.println(index);
        double minTime = 1.557868858E12;
        double maxTime = 1.5579648E12;
        double t1 = minTime + TimeUnit.HOURS.toMillis(12);
        double t2 = t1 + TimeUnit.HOURS.toMillis(1);
        // sydney region
        double[] a = new double[] { -33.68, 150.86, t1 };
        double[] b = new double[] { -34.06, 151.34, t2 };
//        double[] a = new double[] { 0, 100, minTime };
//        double[] b = new double[] { -60, 175, maxTime };
        Bounds bounds = Bounds.create(a, b);
        long t = System.currentTimeMillis();
        long count = index.search(bounds, output).count().get();
        System.out.println(count + " found in " + (System.currentTimeMillis() - t) + "ms");

        t = System.currentTimeMillis();
        long c = searchRaw(bounds);
        System.out.println(c + " found in " + (System.currentTimeMillis() - t) + "ms");
    }

    private static long searchRaw(Bounds bounds) throws IOException, FileNotFoundException {
        long c = 0;
        try (InputStream in = new BufferedInputStream(new FileInputStream(output))) {
            Reader<byte[]> r = ser.createReader(in);
            byte[] bytes;
            while ((bytes = r.read()) != null) {
                double[] p = point.apply(bytes);
                if (bounds.contains(p)) {
                    c++;
                }
            }
        }
        return c;
    }

}
