package com.gmail.a.e.tsvetkov.integrationtest;

import com.gmail.a.e.tsvetkov.driver.sql.executor.SqlExecutor;
import com.sksamuel.elastic4s.http.HttpClient;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.stream.Stream;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestBase {
    static {
        try {
            Class.forName(com.gmail.a.e.tsvetkov.driver.Driver.class.getName());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    protected String host = "localhost";

    @BeforeMethod
    public void cleanupDb() {
        HttpClient connect = SqlExecutor.connect(host);
        SqlExecutor.deleteAllIndexes(connect);
        connect.close();
    }

    protected void assertResult(ResultSet res, Row... rows) throws SQLException {
        for (int i = 0; i < rows.length; i++) {
            Row row = rows[i];
            var r = res.next();
            try {
                assertTrue(r);
                row.assertRow(res);
            } catch (AssertionError a) {
                throw new AssertionError("Missmatch on row " + i + ": " + a.getMessage(), a);
            }
        }
        assertFalse(res.next());
    }

    protected Row row(Cell... columns) {
        return new Row(columns);
    }

    protected Cell i(int value) {
        return new Cell() {
            @Override
            public void assertValue(ResultSet res, int i) throws SQLException {
                int actualValue = res.getInt(i + 1);
                assertEquals(actualValue, value);
            }

            @Override
            public String toSqlLiteral() {
                return String.valueOf(value);
            }
        };
    }

    protected Cell b(boolean value) {
        return new Cell() {
            @Override
            public void assertValue(ResultSet res, int i) throws SQLException {
                boolean actualValue = res.getBoolean(i + 1);
                assertEquals(actualValue, value);
            }

            @Override
            public String toSqlLiteral() {
                return String.valueOf(value);
            }

        };
    }

    public interface Cell {
        void assertValue(ResultSet res, int i) throws SQLException;

        String toSqlLiteral();
    }

    public class Row {
        private final Cell[] columns;

        public Row(Cell[] columns) {
            this.columns = columns;
        }

        public Stream<Cell> getColumns() {
            return Stream.of(columns);
        }

        public void assertRow(ResultSet res) throws SQLException {
            for (int i = 0; i < columns.length; i++) {
                try {
                    columns[i].assertValue(res, i);
                } catch (AssertionError a) {
                    throw new AssertionError("Missmatch on column " + i + ": " + a.getMessage(), a);
                }

            }
        }
    }
}
