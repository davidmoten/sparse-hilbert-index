package com.github.davidmoten.shi.fixes;

import java.nio.charset.StandardCharsets;
import java.util.function.Function;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import com.github.davidmoten.bigsorter.Serializer;

public class FixesCsv {

    public static final Function<CSVRecord, double[]> pointMapper = rec -> {
        // random access means the csv reader doesn't read the header so we have to use
        // index positions
        double lat = Double.parseDouble(rec.get(1));
        double lon = Double.parseDouble(rec.get(2));
        double time = Long.parseLong(rec.get(3));
        return new double[] { lat, lon, time };
    };

    public static final Serializer<CSVRecord> serializer = Serializer.csv( //
            CSVFormat.DEFAULT //
                    .withFirstRecordAsHeader() //
                    .withRecordSeparator("\n"),
            StandardCharsets.UTF_8);

}
