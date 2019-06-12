package com.github.davidmoten.shi;

import org.junit.Test;

import com.github.davidmoten.junit.Asserts;

public class UtilTest {
    
    @Test
    public void isUtilityClass() {
        Asserts.assertIsUtilityClass(Util.class);
    }

}
