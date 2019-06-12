package com.github.davidmoten.shi.fixes;

import static com.github.davidmoten.shi.fixes.Fixes.input;
import static com.github.davidmoten.shi.fixes.Fixes.point;
import static com.github.davidmoten.shi.fixes.Fixes.ser;

import java.io.File;
import java.util.Arrays;
import java.util.function.Function;

import com.github.davidmoten.shi.Index;

public class FixesCreateSpatialIndexMain {

    public static void main(String[] args) {
        Function<byte[], double[]> pt = x -> Arrays.copyOf(point.apply(x), 2);
        Index<byte[]> index = Index//
                .serializer(ser) //
                .pointMapper(pt) //
                .input(input) //
                .output(new File("target/output-spatial")) //
                .bits(10) //
                .dimensions(2) //
                .createIndex() //
                .write(new File("target/output-spatial.idx"));
        System.out.println(index);
    }
}
