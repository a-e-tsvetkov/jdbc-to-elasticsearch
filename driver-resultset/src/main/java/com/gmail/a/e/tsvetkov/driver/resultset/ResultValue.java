package com.gmail.a.e.tsvetkov.driver.resultset;

import java.sql.SQLException;

public interface ResultValue {
    default String getString() throws SQLException {
        throw new SQLException("Wrong type");
    }

    default boolean getBoolean() throws SQLException {
        throw new SQLException("Wrong type");
    }

    default byte getByte() throws SQLException {
        throw new SQLException("Wrong type");
    }

    default short getShort() throws SQLException {
        throw new SQLException("Wrong type");
    }

    default int getInt() throws SQLException {
        throw new SQLException("Wrong type");
    }

    default long getLong() throws SQLException {
        throw new SQLException("Wrong type");
    }

    default float getFloat() throws SQLException {
        throw new SQLException("Wrong type");
    }

    default double getDouble() throws SQLException {
        throw new SQLException("Wrong type");
    }
}
