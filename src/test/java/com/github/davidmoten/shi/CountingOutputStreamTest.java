package com.github.davidmoten.shi;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

public class CountingOutputStreamTest {

    @Test
    public void testWriteSingleByte() throws IOException {
        try (CountingOutputStream out = new CountingOutputStream()) {
            assertEquals(0, out.count());
            out.write(3);
            assertEquals(1, out.count());
        }
    }

}
