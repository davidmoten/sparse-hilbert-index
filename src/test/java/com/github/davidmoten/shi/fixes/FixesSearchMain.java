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
import java.util.concurrent.TimeUnit;

import javax.net.ssl.HttpsURLConnection;

import com.github.davidmoten.bigsorter.Reader;
import com.github.davidmoten.shi.Bounds;
import com.github.davidmoten.shi.Index;

public class FixesSearchMain {

    public static void main(String[] args) throws FileNotFoundException, IOException {
        Index<byte[]> index = Index.serializer(ser).pointMapper(point).read(idx);
        System.out.println(index);
        double minTime = 1.557868858E12;
        double maxTime = 1.5579648E12;
        double t1 = minTime + TimeUnit.HOURS.toMillis(22);
        double t2 = t1 + TimeUnit.MINUTES.toMillis(30);
        // sydney region
        // double[] a = new double[] { -33.68, 150.86, t1 };
        // double[] b = new double[] { -34.06, 151.34, t2 };

        // brisbane region
        double[] a = new double[] { -24.9, 150, t1 };
        double[] b = new double[] { -29.5, 158, t2 };
        // larger region
        // double[] a = new double[] { -10.6, 170, minTime };
        // double[] b = new double[] { -50, 179, maxTime };
        Bounds bounds = Bounds.create(a, b);
        System.out.println("queryBounds=" + bounds);

        long t = System.currentTimeMillis();
        long count = index.search(bounds, output, 100).count().get();
        System.out.println(count + " found in " + (System.currentTimeMillis() - t) + "ms using local file search");

        t = System.currentTimeMillis();
        long c = searchRaw(bounds);
        System.out.println(c + " found in " + (System.currentTimeMillis() - t) + "ms using local file scan");

        String location = new String(
                Base64.getDecoder().decode(
                        "aHR0cHM6Ly9tb3Rlbi1maXhlcy5zMy1hcC1zb3V0aGVhc3QtMi5hbWF6b25hd3MuY29tL291dHB1dC1zb3J0ZWQK"),
                StandardCharsets.UTF_8);

        String locationIdx = new String(Base64.getDecoder().decode(
                "aHR0cHM6Ly9tb3Rlbi1maXhlcy5zMy1hcC1zb3V0aGVhc3QtMi5hbWF6b25hd3MuY29tL291dHB1dC1zb3J0ZWQuaWR4Cg=="),
                StandardCharsets.UTF_8);

        t = System.currentTimeMillis();
        // reread index
        index = Index.read(new DataInputStream(getIndexInputStream(locationIdx)), ser, point);
        System.out.println("read index in " + (System.currentTimeMillis() - t) + "ms");
        URL u = new URL(location);
        t = System.currentTimeMillis();
        long records = index.search(bounds, u, 0).count().get();
        System.out.println(records + " found in " + (System.currentTimeMillis() - t)
                + "ms using search over https (s3), index already loaded");
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
