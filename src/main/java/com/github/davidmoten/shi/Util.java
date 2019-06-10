package com.github.davidmoten.shi;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;

class Util {

    static InputStream bufferedInput(File file, boolean gzipped) throws IOException {
        if (!gzipped) {
            return new BufferedInputStream(new FileInputStream(file));
        } else {
            return new GZIPInputStream(new FileInputStream(file));
        }
    }

    static OutputStream bufferedOutput(File file) throws FileNotFoundException {
        return new BufferedOutputStream(new FileOutputStream(file));
    }

    static InputStream bufferedInput(File file) throws IOException {
        return bufferedInput(file, false);
    }
}
