package com.gmail.a.e.tsvetkov.driver.resultset;

import lombok.Builder;
import lombok.Getter;

import java.sql.Types;

@Builder
@Getter
public class AMetadataColumn {
    private final String label;

    /**
     * SQL type from java.sql.Types
     *
     * @see Types
     */
    private final int sqlType;
}
