package com.github.davidmoten.shi.fixes;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.apache.commons.csv.CSVRecord;

import com.github.davidmoten.shi.Index;

public class FixesSearchCsvMain {

    public static void main(String[] args) {
        Index<CSVRecord> index = Index //
                .serializer(FixesCsv.serializer()) //
                .pointMapper(FixesCsv.pointMapper) //
                .read(new File("target/sorted.csv.idx"));
        double minTime = 1.557868858E12;
        // double maxTime = 1.5579648E12;
        double t1 = minTime + TimeUnit.HOURS.toMillis(12);
        double t2 = t1 + TimeUnit.MINUTES.toMillis(30);
        // sydney region
        double[] a = new double[] { -33.68, 150.86, t1 };
        double[] b = new double[] { -34.06, 151.34, t2 };
        long t = System.currentTimeMillis();
        long count = index.search(a, b).file(new File("target/sorted.csv")).count().get();
        System.out.println("found " + count + " in " + (System.currentTimeMillis() - t) + "ms");
    }

}
