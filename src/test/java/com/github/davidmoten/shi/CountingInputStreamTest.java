package com.github.davidmoten.shi;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.junit.Test;

public class CountingInputStreamTest {

    @Test
    public void test() throws IOException {
        try (CountingInputStream in = new CountingInputStream(
                new ByteArrayInputStream("ab".getBytes(StandardCharsets.UTF_8)), 1000)) {
            assertEquals(97, in.read());
            long x = in.readTimeToFirstByteAndSetToZero();
            assertTrue(x > 100000);
            assertEquals(98, in.read());
            assertEquals(0, in.readTimeToFirstByteAndSetToZero());
            assertEquals(-1, in.read());
        }
    }

}
