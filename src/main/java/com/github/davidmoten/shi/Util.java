package com.github.davidmoten.shi;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

final class Util {
    
    private Util() {
        // prevent instantiation
    }

    static InputStream bufferedInput(File file) throws IOException {
        return new BufferedInputStream(new FileInputStream(file));
    }
}
