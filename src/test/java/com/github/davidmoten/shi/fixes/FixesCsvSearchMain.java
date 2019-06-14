package com.github.davidmoten.shi.fixes;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Base64;

import org.apache.commons.csv.CSVRecord;

import com.github.davidmoten.shi.Bounds;
import com.github.davidmoten.shi.Index;
import com.github.davidmoten.shi.WithStats;

public class FixesCsvSearchMain {

    public static void main(String[] args) throws MalformedURLException {
        String locationIdx = new String(Base64.getDecoder().decode(
                "aHR0cHM6Ly9tb3Rlbi1maXhlcy5zMy1hcC1zb3V0aGVhc3QtMi5hbWF6b25hd3MuY29tLzIwMTkt"
                        + "MDUtMTUuY3N2LmlkeAo="));
        Index<CSVRecord> index = Index //
                .serializer(FixesCsv.serializer) //
                .pointMapper(FixesCsv.pointMapper) //
                .read(new URL(locationIdx));
        long count;
        for (Bounds b : new Bounds[] { Fixes.sydney, Fixes.brisbane, Fixes.qld }) {
            long t = System.currentTimeMillis();
//            long count = index.search(b).file(new File("target/sorted.csv")).count().get();
//            System.out.println("found " + count + " in " + (System.currentTimeMillis() - t) + "ms");

            
            String location = new String(Base64.getDecoder().decode(
                    "aHR0cHM6Ly9tb3Rlbi1maXhlcy5zMy1hcC1zb3V0aGVhc3QtMi5hbWF6b25hd3MuY29tLzIwMTkt"
                            + "MDUtMTUuY3N2Cg=="));
            t = System.currentTimeMillis();
            WithStats<CSVRecord> last = index.search(b).withStats().url(location).last().get().get();
            System.out.println("found " + last + " in " + (System.currentTimeMillis() - t) + "ms");
        }
    }

}
