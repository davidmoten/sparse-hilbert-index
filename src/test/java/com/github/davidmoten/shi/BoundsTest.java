package com.github.davidmoten.shi;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class BoundsTest {

    @Test(expected = IllegalArgumentException.class)
    public void testZeroLengthArrayArgumentThrows() {
        Bounds.create(new double[0], new double[0]);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDifferentLengthArrayArgumentsThrows() {
        Bounds.create(new double[1], new double[2]);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testContainsMustBePassedCorrentLengthArray() {
        Bounds.create(new double[2], new double[2]).contains(new double[3]);
    }

    @Test
    public void testToString() {
        assertTrue(Bounds.create(new double[2], new double[2]).toString().startsWith("Bounds ["));
    }

}
