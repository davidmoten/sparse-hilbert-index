package com.github.davidmoten.shi;

import java.io.IOException;
import java.io.InputStream;

final class CountingInputStream extends InputStream {

    private final InputStream in;
    private long count;
    private long ttfb;

    CountingInputStream(InputStream in) {
        this.in = in;
    }

    @Override
    public int read() throws IOException {
        preRead();
        int v = in.read();
        postRead();
        if (v != -1) {
            count++;
        }
        return v;
    }

    private void preRead() {
        if (ttfb == -1) {
            ttfb = -System.currentTimeMillis();
        }
    }

    private void postRead() {
        if (ttfb < 0) {
            ttfb += System.currentTimeMillis();
        } else {
            ttfb = 0;
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        preRead();
        int n = in.read(b, off, len);
        postRead();
        if (n != -1) {
            count += n;
        }
        return n;
    }

    long count() {
        return count;
    }

    long timeToFirstByte() {
        return ttfb;
    }

}
