package com.gmail.a.e.tsvetkov.driver.resultset;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AResultSetBuilder {
    private Map<String, Integer> columns = new HashMap<>();
    private List<RowBuilder> rows = new ArrayList<>();

    public static AResultSetBuilder builder() {
        return new AResultSetBuilder();
    }

    public void addColumn(int index, String columnName) {
        columns.put(columnName, index);
    }

    public RowBuilder addRow() {
        RowBuilder rowBuilder = new RowBuilder();
        rows.add(rowBuilder);
        return rowBuilder;
    }

    public AResultSet build() {
        return new AResultSet(
                columns,
                rows.stream()
                        .map(RowBuilder::build).
                        toArray(Object[][]::new)
        );
    }

    public static class RowBuilder {
        private List<Object> values = new ArrayList<>();

        public void addCell(Object value) {
            values.add(value);
        }

        Object[] build() {
            return values.toArray(new Object[0]);
        }
    }
}
