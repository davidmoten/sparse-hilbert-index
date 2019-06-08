package com.github.davidmoten.shi;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.davidmoten.hilbert.HilbertCurve;
import org.davidmoten.hilbert.Range;
import org.davidmoten.hilbert.SmallHilbertCurve;

import com.github.davidmoten.guavamini.Preconditions;
import com.github.davidmoten.guavamini.annotations.VisibleForTesting;

public final class Index {

    private static final short VERSION = 1;
    private final TreeMap<Integer, Long> indexPositions;
    private final double[] mins;
    private final double[] maxes;
    private final SmallHilbertCurve hc;
    private final long count;

    Index(TreeMap<Integer, Long> indexPositions, double[] mins, double[] maxes, int bits,
            long count) {
        this.indexPositions = indexPositions;
        this.mins = mins;
        this.maxes = maxes;
        this.count = count;
        this.hc = HilbertCurve.small().bits(bits).dimensions(mins.length);
    }

    @VisibleForTesting
    TreeMap<Integer, Long> indexPositions() {
        return indexPositions;
    }

    /**
     * Fits the desired ranges to the effective querying ranges according to the
     * known index positions.
     * 
     * @param ranges list of ranges in ascending order
     * @return querying ranges based on known index positions
     */
    public List<PositionRange> getPositionRanges(Iterable<Range> ranges) {
        return getPositionRanges(indexPositions, ranges);
    }

    @VisibleForTesting
    static List<PositionRange> getPositionRanges(TreeMap<Integer, Long> indexPositions,
            Iterable<Range> ranges) {
        List<PositionRange> list = new ArrayList<>();
        for (Range range : ranges) {
            if (range.low() <= indexPositions.lastKey()
                    && range.high() >= indexPositions.firstKey()) {
                Long startPosition = value(indexPositions.floorEntry((int) range.low()));
                if (startPosition == null) {
                    startPosition = indexPositions.firstEntry().getValue();
                }
                Long endPosition = value(indexPositions.ceilingEntry((int) range.high()));
                if (endPosition == null) {
                    endPosition = indexPositions.lastEntry().getValue();
                }
                list.add(new PositionRange(Collections.singletonList(range), startPosition,
                        endPosition));
            }
        }
        list.forEach(System.out::println);
        return simplify(list);
    }

    private static <T, R> R value(Entry<T, R> entry) {
        if (entry == null) {
            return null;
        } else {
            return entry.getValue();
        }
    }

    private static List<PositionRange> simplify(List<PositionRange> positionRanges) {
        // TODO conbine with calling method to minimize allocations
        LinkedList<PositionRange> list = new LinkedList<>();
        for (PositionRange p : positionRanges) {
            if (list.isEmpty()) {
                list.add(p);
            } else {
                PositionRange last = list.getLast();
                if (last.overlapsPositionWith(p)) {
                    list.pollLast();
                    list.offer(last.join(p));
                } else {
                    list.offer(p);
                }
            }
        }
        return list;
    }

    public double[] mins() {
        return mins;
    }

    public double[] maxes() {
        return maxes;
    }

    /**
     * Returns count of records in file indexed by this.
     * 
     * @return count of records in file indexed by this.
     */
    public long count() {
        return count;
    }

    public long[] ordinates(double... d) {
        Preconditions.checkArgument(d.length == mins.length);
        long[] x = new long[d.length];
        for (int i = 0; i < d.length; i++) {
            x[i] = Math.round(((Math.min(d[i], maxes[i]) - mins[i]) / (maxes[i] - mins[i]))
                    * hc.maxOrdinate());
        }
        return x;
    }

    public SmallHilbertCurve hilbertCurve() {
        return hc;
    }

    public static Index read(File file) throws FileNotFoundException, IOException {
        try (DataInputStream dis = new DataInputStream(
                new BufferedInputStream(new FileInputStream(file)))) {
            return read(dis);
        }
    }

    public static Index read(DataInputStream dis) throws IOException {
        short version = dis.readShort();
        int bits = dis.readInt();
        int dimensions = dis.readInt();
        double[] mins = new double[dimensions];
        double[] maxes = new double[dimensions];
        for (int i = 0; i < dimensions; i++) {
            mins[i] = dis.readDouble();
            maxes[i] = dis.readDouble();
        }
        long count = dis.readLong();
        int numEntries = dis.readInt();
        dis.readInt();
        TreeMap<Integer, Long> indexPositions = new TreeMap<Integer, Long>();
        for (int i = 0; i < numEntries; i++) {
            int pos = dis.readInt();
            int index = dis.readInt();
            indexPositions.put(index, (long) pos);
        }
        return new Index(indexPositions, mins, maxes, bits, count);
    }

    public void write(File idx) throws FileNotFoundException, IOException {
        try (DataOutputStream dos = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream(idx)))) {
            dos.writeShort(VERSION);
            dos.writeInt(hc.bits());
            dos.writeInt(hc.dimensions());
            for (int i = 0; i < hc.dimensions(); i++) {
                dos.writeDouble(mins[i]);
                dos.writeDouble(maxes[i]);
            }

            dos.writeLong(count);

            // num index entries
            dos.writeInt(indexPositions.size());

            // write 0 for int position
            // write 1 for long position
            dos.writeInt(0);

            for (Entry<Integer, Long> entry : indexPositions.entrySet()) {
                dos.writeInt(entry.getKey());
                Long pos = entry.getValue();
                if (pos > Integer.MAX_VALUE) {
                    throw new RuntimeException(
                            "file size too big for integer positions in index entries");
                }
                dos.writeInt(entry.getValue().intValue());
            }
        }
    }

    @Override
    public String toString() {
        return "Index [mins=" + Arrays.toString(mins) + ", maxes=" + Arrays.toString(maxes) + "]";
    }

    public int numEntries() {
        return indexPositions.size();
    }

}
