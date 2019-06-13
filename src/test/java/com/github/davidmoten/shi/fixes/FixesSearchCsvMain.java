package com.github.davidmoten.shi.fixes;

import java.io.File;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

import org.apache.commons.csv.CSVRecord;

import com.github.davidmoten.shi.Bounds;
import com.github.davidmoten.shi.Index;

public class FixesSearchCsvMain {

    public static void main(String[] args) {
        Index<CSVRecord> index = Index //
                .serializer(FixesCsv.serializer) //
                .pointMapper(FixesCsv.pointMapper) //
                .read(new File("target/sorted.csv.idx"));
        Bounds b = Fixes.sydney;
        long t = System.currentTimeMillis();
        long count = index.search(b).file(new File("target/sorted.csv")).count().get();
        System.out.println("found " + count + " in " + (System.currentTimeMillis() - t) + "ms");

        String locationIdx = new String(Base64.getDecoder().decode(
                "aHR0cHM6Ly9tb3Rlbi1maXhlcy5zMy1hcC1zb3V0aGVhc3QtMi5hbWF6b25hd3MuY29tLzIwMTkt"
                        + "MDUtMTUuY3N2LmlkeAo="));
        String location = new String(Base64.getDecoder().decode(
                "aHR0cHM6Ly9tb3Rlbi1maXhlcy5zMy1hcC1zb3V0aGVhc3QtMi5hbWF6b25hd3MuY29tLzIwMTkt"
                        + "MDUtMTUuY3N2Cg=="));
        t = System.currentTimeMillis();
        count = index.search(b).url(location).count().get();
        System.out.println("found " + count + " in " + (System.currentTimeMillis() - t) + "ms");
    }

}
