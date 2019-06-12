package com.github.davidmoten.shi;

import org.junit.Test;

public class PositionRangeTest {

    @Test(expected = IllegalArgumentException.class)
    public void testMaxHilbertIndexIsNegativeThrows() {
        new PositionRange(-1, 0, 0);
    }

}
