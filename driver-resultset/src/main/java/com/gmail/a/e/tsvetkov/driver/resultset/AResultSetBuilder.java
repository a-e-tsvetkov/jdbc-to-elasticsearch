package com.gmail.a.e.tsvetkov.driver.resultset;

import com.gmail.a.e.tsvetkov.driver.resultset.AMetadataColumn.AMetadataColumnBuilder;

import java.math.BigDecimal;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;

public class AResultSetBuilder {
    private MetadataBuilder metadataBuilder = new MetadataBuilder();
    private List<RowBuilder> rows = new ArrayList<>();

    public static AResultSetBuilder builder() {
        return new AResultSetBuilder();
    }

    public MetadataBuilder getMetadataBuilder() {
        return metadataBuilder;
    }

    public RowBuilder addRow() {
        RowBuilder rowBuilder = new RowBuilder();
        rows.add(rowBuilder);
        return rowBuilder;
    }

    public AResultSet build() {
        return new AResultSet(
                metadataBuilder.build(),
                rows.stream()
                        .map(RowBuilder::build).
                        toArray(ResultValue[][]::new)
        );
    }

    public static class RowBuilder {
        private List<ResultValue> values = new ArrayList<>();

        public void addCell(boolean value) {
            values.add(new ResultValueBoolean(value));
        }

        public void addCell(String value) {
            values.add(new ResultValueString(value));
        }

        public void addCell(BigDecimal value) {
            values.add(new ResultValueBigDecimal(value));
        }

        ResultValue[] build() {
            return values.toArray(new ResultValue[0]);
        }
    }


    public static class MetadataBuilder {
        private List<AMetadataColumnBuilder> columns = new ArrayList<>();

        public AMetadataColumnBuilder addColumn() {
            AMetadataColumnBuilder builder = AMetadataColumn.builder();
            columns.add(builder);
            return builder;
        }

        ResultSetMetaData build() {
            return new AMetadata(
                    columns.stream()
                            .map(AMetadataColumnBuilder::build)
                            .toArray(AMetadataColumn[]::new));
        }
    }
}
