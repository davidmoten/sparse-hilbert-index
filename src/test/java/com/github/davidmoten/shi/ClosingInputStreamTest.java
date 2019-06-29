package com.github.davidmoten.shi;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

public class ClosingInputStreamTest {

    @Test
    public void test() throws IOException {
        AtomicBoolean closed = new AtomicBoolean();
        InputStream in = new InputStream() {

            @Override
            public int read() throws IOException {
                return 1;
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                return 2;
            }

        };
        ClosingInputStream c = new ClosingInputStream(in, () -> closed.set(true));
        assertEquals(1, c.read());
        assertEquals(2, c.read(new byte[10], 3, 2));
        assertFalse(closed.get());
        c.close();
        assertTrue(closed.get());
    }

    @Test(expected = RuntimeException.class)
    public void testThrows() throws IOException {
        InputStream in = new InputStream() {

            @Override
            public int read() throws IOException {
                return 1;
            }

        };
        ClosingInputStream c = new ClosingInputStream(in, () -> {
            throw new IOException("boo");
        });
        c.close();
    }

}
