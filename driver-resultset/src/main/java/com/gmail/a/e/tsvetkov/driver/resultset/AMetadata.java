package com.gmail.a.e.tsvetkov.driver.resultset;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

//@Builder
public class AMetadata extends AWrapper implements ResultSetMetaData {

    private final AMetadataColumn[] columns;

    AMetadata(AMetadataColumn[] columns) {
        this.columns = columns;
    }

    int getColumnIndex(String columnLabel) throws SQLException {
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].getLabel().equals(columnLabel)) {
                return i;
            }
        }
        throw new SQLException("unknown column");
    }

    private AMetadataColumn getColumn(int column) throws SQLException {
        if (column <= 0 || column > columns.length) {
            throw new SQLException("Illegal column number");
        }
        return columns[column - 1];
    }

    @Override
    public int getColumnCount() {
        return columns.length;
    }

    @Override
    public boolean isAutoIncrement(int column) {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) {
        return false;
    }

    @Override
    public boolean isSearchable(int column) {
        return false;
    }

    @Override
    public boolean isCurrency(int column) {
        return false;
    }

    @Override
    public int isNullable(int column) {
        return 0;
    }

    @Override
    public boolean isSigned(int column) {
        return false;
    }

    @Override
    public int getColumnDisplaySize(int column) {
        return 0;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumn(column).getLabel();
    }

    @Override
    public String getColumnName(int column) {
        return null;
    }

    @Override
    public String getSchemaName(int column) {
        return null;
    }

    @Override
    public int getPrecision(int column) {
        return 0;
    }

    @Override
    public int getScale(int column) {
        return 0;
    }

    @Override
    public String getTableName(int column) {
        return null;
    }

    @Override
    public String getCatalogName(int column) {
        return null;
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return getColumn(column).getSqlType();
    }

    @Override
    public String getColumnTypeName(int column) {
        return null;
    }

    @Override
    public boolean isReadOnly(int column) {
        return false;
    }

    @Override
    public boolean isWritable(int column) {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) {
        return false;
    }

    @Override
    public String getColumnClassName(int column) {
        return null;
    }
}
