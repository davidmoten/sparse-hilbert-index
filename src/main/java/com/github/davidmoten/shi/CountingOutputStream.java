package com.github.davidmoten.shi;

import java.io.IOException;
import java.io.OutputStream;

class CountingOutputStream extends OutputStream {

    private long count;

    @Override
    public void write(int b) throws IOException {
        count++;
    }

    public long count() {
        return count;
    }

}
