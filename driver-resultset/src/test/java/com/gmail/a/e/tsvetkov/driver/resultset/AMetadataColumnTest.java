package com.gmail.a.e.tsvetkov.driver.resultset;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class AMetadataColumnTest {

    @Test
    public void testBuilder() {
        AMetadataColumn column = AMetadataColumn.builder()
                .label("a")
                .build();

        assertEquals(column.getLabel(), "a");
    }
}