package com.github.davidmoten.shi;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.junit.Test;

public class LimitingInputStreamTest {

    @Test
    public void test() throws IOException {
        LimitingInputStream in = new LimitingInputStream(new ByteArrayInputStream(new byte[100]), 2);
        assertEquals(0, in.read());
        assertEquals(0, in.read());
        assertEquals(-1, in.read());
    }

    @Test
    public void test2() throws IOException {
        LimitingInputStream in = new LimitingInputStream(new ByteArrayInputStream(new byte[2]), 100);
        assertEquals(0, in.read());
        assertEquals(0, in.read());
        assertEquals(-1, in.read());
    }

    @Test
    public void test3() throws IOException {
        LimitingInputStream in = new LimitingInputStream(new ByteArrayInputStream(new byte[100]), 2);
        byte[] b = new byte[1];
        assertEquals(1, in.read(b));
        assertEquals(1, in.read(b));
        assertEquals(-1, in.read(b));
        assertEquals(-1, in.read(b));
    }

    @Test
    public void test4() throws IOException {
        LimitingInputStream in = new LimitingInputStream(new ByteArrayInputStream(new byte[100]), 9);
        byte[] b = new byte[5];
        assertEquals(5, in.read(b));
        assertEquals(4, in.read(b));
        assertEquals(-1, in.read(b));
        assertEquals(-1, in.read(b));
    }

}
