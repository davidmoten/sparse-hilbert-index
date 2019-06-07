package com.github.davidmoten.shi;

import java.io.IOException;
import java.io.InputStream;

class LimitingInputStream extends InputStream {

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
        if (v!= -1) {
            count++;
        }
        return v;
    }

    
    
}
