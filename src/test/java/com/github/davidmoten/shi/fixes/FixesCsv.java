package com.github.davidmoten.shi.fixes;

import java.nio.charset.StandardCharsets;
import java.util.function.Function;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import com.github.davidmoten.bigsorter.Serializer;

public class FixesCsv {

    public static final Function<CSVRecord, double[]> pointMapper = rec -> {
        double lat = Double.parseDouble(rec.get(1));
        double lon = Double.parseDouble(rec.get(2));
        double time = Long.parseLong(rec.get(3));
        return new double[] { lat, lon, time };
    };

    public static final Serializer<CSVRecord> serializer = Serializer
            .csv(CSVFormat.DEFAULT.withFirstRecordAsHeader(), StandardCharsets.UTF_8);

}
