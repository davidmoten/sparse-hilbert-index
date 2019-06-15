package com.github.davidmoten.shi;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class WithStatsTest {

    @Test
    public void testGetters() {
        WithStats<Integer> w = new WithStats<>(1, 100, 200, 123, 456, 6, 890);
        assertTrue(w.hasValue());
        assertEquals(1, (int) w.value());
        assertEquals(100, (int) w.recordsRead());
        assertEquals(200, (int) w.recordsFound());
        assertEquals(123, (int) w.bytesRead());
        assertEquals(456, (int) w.timeToFirstByteMs());
        assertEquals(76, (int) w.timeToFirstByteMsAverage());
        assertEquals(6, (int) w.chunksRead());
    }

    @Test
    public void testHasValueWhenEmpty() {
        WithStats<Integer> w = new WithStats<>(null, 100, 200, 123, 456, 6, 890);
        assertFalse(w.hasValue());
    }

}
