package com.github.davidmoten.shi;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

class Util {

    static InputStream bufferedInput(File file) throws FileNotFoundException {
        return new BufferedInputStream(new FileInputStream(file));
    }
    
    static OutputStream bufferedOutput(File file) throws FileNotFoundException {
        return new BufferedOutputStream(new FileOutputStream(file));
    }
}
