package com.github.davidmoten.shi;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

import org.davidmoten.hilbert.Range;
import org.junit.Test;

import com.github.davidmoten.bigsorter.Serializer;
import com.github.davidmoten.guavamini.Lists;

public class IndexTest {

    @Test
    public void test() {
        TreeMap<Integer, Long> map = new TreeMap<>();
        map.put(237, 0L);
        map.put(472177237, 4082820L);
        Index<String> index = new Index<String>(map,
                new double[] { -85.14174, -115.24912, 1557868858000L },
                new double[] { 47.630283, 179.99948, 1557964800000L }, 10, 2, Serializer.linesUtf8(), x -> new double[] {0,0,0});
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

    @Test
    public void testGetPositionRanges1() {
        TreeMap<Integer, Long> map = new TreeMap<>();
        map.put(1, 0L);
        map.put(8, 5L);
        map.put(16, 10L);
        map.put(20, 16L);
        List<PositionRange> ranges = Index.getPositionRanges(map,
                Collections.singletonList(Range.create(5, 9)));
        System.out.println(ranges);
        assertEquals(1, ranges.size());
        PositionRange pr = ranges.get(0);
        assertEquals(0, pr.floorPosition());
        assertEquals(10, pr.ceilingPosition());
    }

    @Test
    public void testGetPositionRanges2() {
        TreeMap<Integer, Long> map = new TreeMap<>();
        map.put(1, 0L);
        map.put(8, 5L);
        map.put(16, 10L);
        map.put(20, 16L);
        List<PositionRange> ranges = Index.getPositionRanges(map,
                Lists.newArrayList(Range.create(2, 3), Range.create(18, 22)));
        System.out.println(ranges);
        assertEquals(2, ranges.size());
        {
            PositionRange pr = ranges.get(0);
            assertEquals(0, pr.floorPosition());
            assertEquals(5, pr.ceilingPosition());
        }
        {
            PositionRange pr = ranges.get(1);
            assertEquals(10, pr.floorPosition());
            assertEquals(16, pr.ceilingPosition());
        }
    }
}
