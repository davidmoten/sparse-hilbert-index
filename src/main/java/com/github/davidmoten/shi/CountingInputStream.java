package com.github.davidmoten.shi;

import java.io.IOException;
import java.io.InputStream;

final class CountingInputStream extends InputStream {

    private final InputStream in;
    private long count;
    private long startTime;
    private long ttfb;

    CountingInputStream(InputStream in, long startTime) {
        this.in = in;
        this.startTime = startTime;
    }

    @Override
    public int read() throws IOException {
        int v = in.read();
        postRead();
        if (v != -1) {
            count++;
        }
        return v;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int n = in.read(b, off, len);
        postRead();
        if (n != -1) {
            count += n;
        }
        return n;
    }

    private void postRead() {
        if (startTime != -1) {
            ttfb = System.currentTimeMillis() - startTime;
            startTime = -1;
        }
    }

    long count() {
        long c = count;
        count = 0;
        return c;
    }

    long readTimeToFirstByteAndSetToZero() {
        long v = ttfb;
        ttfb = 0;
        return v;
    }

}
