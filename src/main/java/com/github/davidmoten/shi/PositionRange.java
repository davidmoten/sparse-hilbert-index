package com.github.davidmoten.shi;

import com.github.davidmoten.guavamini.Preconditions;

public final class PositionRange {

    // hilbert curve index ranges covered by this position range
    private final long maxHilbertIndex;

    // highest known position with index less than or equal to lowIndex or the
    // lowest known position if nothing lower
    private final long floorPosition;

    // lowest known position with index less than or equal to highIndex or the
    // highest known position if nothing higher
    private final long ceilingPosition;

    public PositionRange(long maxHilbertIndex, long floorPosition, long ceilingPosition) {
        Preconditions.checkArgument(maxHilbertIndex >= 0);
        this.maxHilbertIndex = maxHilbertIndex;
        this.floorPosition = floorPosition;
        this.ceilingPosition = ceilingPosition;
    }

    public long maxHilbertIndex() {
        return maxHilbertIndex;
    }

    public long floorPosition() {
        return floorPosition;
    }

    public long ceilingPosition() {
        return ceilingPosition;
    }

    public PositionRange join(PositionRange other) {

        return new PositionRange(Math.max(maxHilbertIndex, other.maxHilbertIndex), //
                Math.min(floorPosition, other.floorPosition), //
                Math.max(ceilingPosition, other.ceilingPosition));
    }

    @Override
    public String toString() {
        return "PositionRange [maxHilbertIndex=" + maxHilbertIndex + ", floorPosition="
                + floorPosition + ", ceilingPosition=" + ceilingPosition + "]";
    }

}
