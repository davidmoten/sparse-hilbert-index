package com.github.davidmoten.shi;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

import org.davidmoten.hilbert.Range;
import org.junit.Test;

public class IndexTest {

    @Test
    public void test() {
        TreeMap<Integer, Long> map = new TreeMap<>();
        map.put(237, 0L);
        map.put(472177237, 4082820L);
        Index index = new Index(map, new double[] { -85.14174, -115.24912, 1557868858000L },
                new double[] { 47.630283, 179.99948, 1557964800000L }, 10, 2);
        long[] o = index.ordinates(-84.23007, -115.24912, 1557964123000L);
        assertEquals(153391853, index.hilbertCurve().index(o));
    }

    @Test
    public void testGetPositionRangesForEmptyRanges() {
        TreeMap<Integer, Long> map = new TreeMap<>();
        map.put(1, 0L);
        map.put(8, 5L);
        List<PositionRange> ranges = Index.getPositionRanges(map, Collections.emptyList());
        assertTrue(ranges.isEmpty());
    }

    @Test
    public void testGetPositionRangesSingleRangeAboveMax() {
        TreeMap<Integer, Long> map = new TreeMap<>();
        map.put(1, 0L);
        map.put(8, 5L);
        List<PositionRange> ranges = Index.getPositionRanges(map,
                Collections.singletonList(Range.create(10, 12)));
        assertTrue(ranges.isEmpty());
    }

    @Test
    public void testGetPositionRangesSingleRangeBelowMin() {
        TreeMap<Integer, Long> map = new TreeMap<>();
        map.put(1, 0L);
        map.put(8, 5L);
        List<PositionRange> ranges = Index.getPositionRanges(map,
                Collections.singletonList(Range.create(-3, -1)));
        assertTrue(ranges.isEmpty());
    }
}
