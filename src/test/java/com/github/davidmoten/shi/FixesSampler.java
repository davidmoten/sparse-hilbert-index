package com.github.davidmoten.shi;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;

import com.github.davidmoten.bigsorter.Reader;
import com.github.davidmoten.bigsorter.Serializer;

public class FixesSampler {

    public static void main(String[] args) throws IOException {
        String filename = "2019-05-15.binary-fixes-with-mmsi.gz";
        
        final int recordSize = 35;
        File input = new File(System.getProperty("user.home") + "/Downloads/" + filename);
        File sampled = new File("target/2019-05-15.binary-fixes-with-mmsi.sampled.1000");
        
        Serializer<byte[]> ser = Serializer.fixedSizeRecord(recordSize);
        try (InputStream in = new BufferedInputStream(new GZIPInputStream(new FileInputStream(input)));
                OutputStream out = new BufferedOutputStream(new FileOutputStream(sampled))) {
            Reader<byte[]> reader = ser.createReader(in);
            byte[] bytes;
            int count = 0;
            while ((bytes = reader.read()) != null) {
                count++;
                if (count % 400 == 0) {
                    count = 0;
                    out.write(bytes);
                }
            }
        }
        System.out.println("done");
    }

}
