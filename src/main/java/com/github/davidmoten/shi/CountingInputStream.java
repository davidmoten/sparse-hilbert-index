package com.github.davidmoten.shi;

import java.io.IOException;
import java.io.InputStream;

final class CountingInputStream extends InputStream {

    private final InputStream in;
    private long count;

    CountingInputStream(InputStream in) {
        this.in = in;
    }

    @Override
    public int read() throws IOException {
        int v = in.read();
        if (v != -1) {
            count++;
        }
        return v;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int n = in.read(b, off, len);
        if (n != -1) {
            count += n;
        }
        return n;
    }

    long count() {
        return count;
    }

}
