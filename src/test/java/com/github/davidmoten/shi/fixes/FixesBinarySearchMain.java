package com.github.davidmoten.shi.fixes;

import static com.github.davidmoten.shi.fixes.Fixes.idx;
import static com.github.davidmoten.shi.fixes.Fixes.output;
import static com.github.davidmoten.shi.fixes.Fixes.point;
import static com.github.davidmoten.shi.fixes.Fixes.ser;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.net.ssl.HttpsURLConnection;

import com.github.davidmoten.bigsorter.Reader;
import com.github.davidmoten.shi.Bounds;
import com.github.davidmoten.shi.Index;

public class FixesBinarySearchMain {

    public static void main(String[] args) throws FileNotFoundException, IOException {
        Index<byte[]> index = Index.serializer(ser).pointMapper(point).read(idx);
        System.out.println(index);
        double minTime = 1.557868858E12;
        // double maxTime = 1.5579648E12;
        double t1 = minTime + TimeUnit.HOURS.toMillis(12);
        double t2 = t1 + TimeUnit.MINUTES.toMillis(30);
        // sydney region
        double[] a = new double[] { -33.68, 150.86, t1 };
        double[] b = new double[] { -34.06, 151.34, t2 };
        Bounds sydney = Bounds.create(new double[] { -33.68, 150.86, t1 },
                new double[] { -34.06, 151.34, t2 });
        // queryBounds=Bounds [mins=[-34.06, 150.86, 1.557948058E12], maxes=[-33.68,
        // 151.34, 1.557951658E12]]
        // 1667 found in 59ms using local file search
        // 1667 found in 932ms using local file scan
        // read index in 483ms
        // 1667 found in 326ms using search over https (s3), index already loaded

        // brisbane region
        // double[] a = new double[] { -24.9, 150, t1 };
        // double[] b = new double[] { -29.5, 158, t2 };
        // queryBounds=Bounds [mins=[-29.5, 150.0, 1.557948058E12], maxes=[-24.9, 158.0,
        // 1.557951658E12]]
        // 38319 found in 120ms using local file search
        // 38319 found in 1012ms using local file scan
        // read index in 489ms
        // 38319 found in 875ms using search over https (s3), index already loaded

        // QLD region
        // double[] a = new double[] { -9.481, 137.3, t1 };
        // double[] b = new double[] { -29.0, 155.47, t2 };
        // queryBounds=Bounds [mins=[-29.0, 137.3, 1.557948058E12], maxes=[-9.481,
        // 155.47, 1.557951658E12]]
        // 166229 found in 270ms using local file search
        // 166229 found in 1040ms using local file scan
        // read index in 516ms
        // 166229 found in 3258ms using search over https (s3), index already loaded

        // TAS region
//        double[] a = new double[] { -39.389, 143.491, t1 };
//        double[] b = new double[] { -44, 149.5, t2 };
        // queryBounds=Bounds [mins=[-44.0, 143.491, 1.557948058E12], maxes=[-39.389,
        // 149.5, 1.557951658E12]]
        // 6255 found in 95ms using local file search
        // 6255 found in 939ms using local file scan
        // read index in 508ms
        // 6255 found in 609ms using search over https (s3), index already loaded

        // larger region
        // double[] a = new double[] { -10.6, 170, minTime };
        // double[] b = new double[] { -50, 179, maxTime };
        Bounds bounds = Bounds.create(a, b);
        System.out.println("queryBounds=" + bounds);

        long t = System.currentTimeMillis();
        long count = index.search(bounds).maxRanges(100).file(output).count().get();
        System.out.println(count + " found in " + (System.currentTimeMillis() - t)
                + "ms using local file search");

        t = System.currentTimeMillis();
        long c = searchRaw(bounds);
        System.out.println(
                c + " found in " + (System.currentTimeMillis() - t) + "ms using local file scan");

        String location = new String(Base64.getDecoder().decode(
                "aHR0cHM6Ly9tb3Rlbi1maXhlcy5zMy1hcC1zb3V0aGVhc3QtMi5hbWF6b25hd3MuY29tL291dHB1dC1zb3J0ZWQK"),
                StandardCharsets.UTF_8);

        String locationIdx = new String(Base64.getDecoder().decode(
                "aHR0cHM6Ly9tb3Rlbi1maXhlcy5zMy1hcC1zb3V0aGVhc3QtMi5hbWF6b25hd3MuY29tL291dHB1dC1zb3J0ZWQuaWR4Cg=="),
                StandardCharsets.UTF_8);

        t = System.currentTimeMillis();
        // reread index
        try (DataInputStream in = new DataInputStream(getIndexInputStream(locationIdx))) {
            index = Index.serializer(ser).pointMapper(point).read(in);
            System.out.println("read index in " + (System.currentTimeMillis() - t) + "ms");
            t = System.currentTimeMillis();
            long records = index.search(bounds).maxRanges(0).url(location).count().get();
            System.out.println(records + " found in " + (System.currentTimeMillis() - t)
                    + "ms using search over https (s3), index already loaded");
        }

        // runHistoricalSearches(index, minTime, u);
    }

    private static void runHistoricalSearches(Index<byte[]> index, double minTime, URL u)
            throws IOException {
        Bounds bounds;
        long t;
        long records;
        for (String line : Files
                .readAllLines(new File("src/test/resources/searches.txt").toPath())) {
            if (line.trim().length() > 0) {
                String[] elems = line.split("\t");
                double hours = Double.parseDouble(elems[0]);
                if (!elems[1].contains(";")) {
                    // circle
                    String[] vals = elems[1].split(",");
                    double lon = Double.parseDouble(vals[0]);
                    double lat = Double.parseDouble(vals[1]);
                    double radiusNm = Double.parseDouble(vals[2]);
                    double lat1 = lat + radiusNm / 60;
                    double lat2 = lat - radiusNm / 60;
                    double lon1 = lon + radiusNm / 60;
                    double lon2 = lon - radiusNm / 60;
                    bounds = Bounds.create(new double[] { lat1, lon1, minTime },
                            new double[] { lat2, lon2, minTime + hours });
                    t = System.currentTimeMillis();
                    records = index.search(bounds).url(u).count().get();
                    System.out.println(records + " found in " + (System.currentTimeMillis() - t)
                            + "ms using search over https (s3), index already loaded");

                } else {
                    String[] pairs = elems[1].split(";");
                    List<Double> pair1 = splitByComma(pairs[0]);
                    List<Double> pair3 = splitByComma(pairs[2]);
                    double lon1 = pair1.get(0);
                    double lat1 = pair1.get(1);
                    double lon2 = pair3.get(0);
                    double lat2 = pair3.get(1);
                    bounds = Bounds.create(new double[] { lat1, lon1, minTime },
                            new double[] { lat2, lon2, minTime + hours });
                    t = System.currentTimeMillis();
                    records = index.search(bounds).url(u).count().get();
                    System.out.println(records + " found in " + (System.currentTimeMillis() - t)
                            + "ms using search over https (s3), index already loaded");
                }
            }
        }
    }

    private static List<Double> splitByComma(String s) {
        return Arrays.stream(s.split(",")).map(Double::parseDouble).collect(Collectors.toList());
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
