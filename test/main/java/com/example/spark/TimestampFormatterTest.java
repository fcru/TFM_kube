package com.example.spark;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class TimestampFormatterTest {
    @Test
    public void testFormatTimestamp() {
        // Test case 1: Specific timestamp
        long timestamp1 = 1723233655000L;
        String expected1 = "year=2024/month=08/day=09/hour=22";
        String result1 = SparkJob.getTimePartition(timestamp1);
        assertEquals(expected1, result1);
    }
}
