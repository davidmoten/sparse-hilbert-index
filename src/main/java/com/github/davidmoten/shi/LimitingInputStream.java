package com.github.davidmoten.shi;

import java.io.IOException;
import java.io.InputStream;

final class LimitingInputStream extends InputStream {

    private final InputStream in;
    private final long limit;
    private long count;

    LimitingInputStream(InputStream in, long limit) {
        this.in = in;
        this.limit = limit;
    }

    @Override
    public int read() throws IOException {
        if (count == limit) {
            return -1;
        }
        int v = in.read();
        if (v != -1) {
            count++;
        }
        return v;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (count == limit) {
            return -1;
        }
        int v = in.read(b, off, len);
        if (v == -1) {
            return v;
        } else {
            long c2 = count + v;
            if (c2 > limit) {
                v = (int) (limit - count);
                count = limit;
            } else {
                count = c2;
            }
            return v;
        }
    }

}
