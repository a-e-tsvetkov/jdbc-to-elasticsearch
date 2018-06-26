package com.gmail.a.e.tsvetkov.integrationtest;

import com.gmail.a.e.tsvetkov.driver.sql.executor.SqlExecutor;
import com.sksamuel.elastic4s.http.HttpClient;
import org.testng.annotations.BeforeMethod;

import java.sql.ResultSet;
import java.sql.SQLException;

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

    protected void assertResult(ResultSet res, RowAsserter... rows) throws SQLException {
        for (RowAsserter row : rows) {
            var r = res.next();
            assertTrue(r);
            row.assertRow(res);
        }
        assertFalse(res.next());
    }

    protected RowAsserter row(ValueAsserter... columns) {
        return new RowAsserter(columns);
    }

    protected ValueAsserter i(int value) {
        return (res, i) -> {
            int actualValue = res.getInt(i + 1);
            assertEquals(actualValue, value);
        };
    }

    public interface ValueAsserter {
        void assertValue(ResultSet res, int i) throws SQLException;
    }

    public class RowAsserter {
        private final ValueAsserter[] columns;

        public RowAsserter(ValueAsserter[] columns) {
            this.columns = columns;
        }

        public void assertRow(ResultSet res) throws SQLException {
            for (int i = 0; i < columns.length; i++) {
                columns[i].assertValue(res, i);
            }
        }
    }
}
