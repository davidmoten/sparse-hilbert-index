package com.github.davidmoten.shi.fixes;

import static com.github.davidmoten.shi.fixes.Fixes.idx;
import static com.github.davidmoten.shi.fixes.Fixes.output;
import static com.github.davidmoten.shi.fixes.Fixes.point;
import static com.github.davidmoten.shi.fixes.Fixes.ser;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.HttpsURLConnection;

import org.davidmoten.kool.function.BiFunction;

import com.github.davidmoten.bigsorter.Reader;
import com.github.davidmoten.shi.Bounds;
import com.github.davidmoten.shi.Index;

public class FixesSearchMain {

    public static void main(String[] args) throws FileNotFoundException, IOException {
        Index<byte[]> index = Index.serializer(ser).pointMapper(point).read(idx);
        System.out.println(index);
        double minTime = 1.557868858E12;
        double maxTime = 1.5579648E12;
        double t1 = minTime + TimeUnit.HOURS.toMillis(0);
        double t2 = t1 + TimeUnit.MINUTES.toMillis(30);
        // sydney region
        // double[] a = new double[] { -33.68, 150.86, t1 };
        // double[] b = new double[] { -34.06, 151.34, t2 };
        double[] a = new double[] { -10.6, 109, minTime };
        double[] b = new double[] { -50, 179, maxTime };
        Bounds bounds = Bounds.create(a, b);
        long t;
        if (true) {
            System.out.println("queryBounds=" + bounds);
            t = System.currentTimeMillis();
            long count = index.search(bounds, output, 100).count().get();
            System.out.println(count + " found in " + (System.currentTimeMillis() - t) + "ms using file search");
        }

        t = System.currentTimeMillis();
        long c = searchRaw(bounds);
        System.out.println(c + " found in " + (System.currentTimeMillis() - t) + "ms using file scan");

        String location = new String(
                Base64.getDecoder().decode(
                        "aHR0cHM6Ly9tb3Rlbi1maXhlcy5zMy1hcC1zb3V0aGVhc3QtMi5hbWF6b25hd3MuY29tL291dHB1dC1zb3J0ZWQK"),
                StandardCharsets.UTF_8);

        String locationIdx = new String(Base64.getDecoder().decode(
                "aHR0cHM6Ly9tb3Rlbi1maXhlcy5zMy1hcC1zb3V0aGVhc3QtMi5hbWF6b25hd3MuY29tL291dHB1dC1zb3J0ZWQuaWR4Cg=="),
                StandardCharsets.UTF_8);

        t = System.currentTimeMillis();
        DataInputStream in3 = new DataInputStream(getIndexInputStream(locationIdx));
        Index<byte[]> ind = Index.read(in3, ser, point);
        System.out.println("read index in " + (System.currentTimeMillis() - t) + "ms");
        URL u = new URL(location);
        System.out.println("opening connection to " + u);
        BiFunction<Long, Optional<Long>, InputStream> factory = (start, end) -> {
            HttpsURLConnection con = (HttpsURLConnection) u.openConnection();
            String bytesRange;
            if (end.isPresent()) {
                bytesRange = "bytes=" + start + "-" + end.get();
            } else {
                bytesRange = "bytes=" + start;
            }
            System.out.println(bytesRange);
            con.addRequestProperty("Range", bytesRange);
            return new BufferedInputStream(con.getInputStream());
        };
        long records = index.search(bounds, factory, 0).count().get();
        System.out.println(
                "found " + records + " in " + (System.currentTimeMillis() - t) + "ms using search over https (s3)");
    }

    private static InputStream getIndexInputStream(String indexUrl) throws IOException {
        URL u = new URL(indexUrl);
        HttpsURLConnection c = (HttpsURLConnection) u.openConnection();
        return c.getInputStream();
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
