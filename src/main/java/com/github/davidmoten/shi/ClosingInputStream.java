package com.github.davidmoten.shi;

import java.io.IOException;
import java.io.InputStream;

import io.reactivex.functions.Action;

class ClosingInputStream extends InputStream {

    private final InputStream in;
    private final Action action;

    ClosingInputStream(InputStream in, Action action) {
        this.in = in;
        this.action = action;
    }

    @Override
    public int read() throws IOException {
        return in.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return in.read(b, off, len);
    }

    @Override
    public void close() throws IOException {
        try {
            in.close();
        } finally {
            try {
                action.run();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
