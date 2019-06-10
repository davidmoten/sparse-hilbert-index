package com.github.davidmoten.shi;

import static com.github.davidmoten.shi.Fixes.idx;
import static com.github.davidmoten.shi.Fixes.input;
import static com.github.davidmoten.shi.Fixes.output;
import static com.github.davidmoten.shi.Fixes.point;
import static com.github.davidmoten.shi.Fixes.ser;

import java.io.IOException;

public class FixesCreateIndex {

    public static void main(String[] args) throws IOException {

        Index<byte[]> index = Index//
                .serializer(ser) //
                .pointMapper(point) //
                .input(input) //
                .output(output) //
                .bits(10) //
                .dimensions(3) //
                .createIndex() //
                .write(idx);
        System.out.println(index);
    }

}
