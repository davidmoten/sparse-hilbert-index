package com.github.davidmoten.shi.fixes;

import static com.github.davidmoten.shi.fixes.Fixes.idx;
import static com.github.davidmoten.shi.fixes.Fixes.input;
import static com.github.davidmoten.shi.fixes.Fixes.output;
import static com.github.davidmoten.shi.fixes.Fixes.point;
import static com.github.davidmoten.shi.fixes.Fixes.ser;

import java.io.IOException;

import com.github.davidmoten.shi.Index;

public class FixesBinaryCreateMain {

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
