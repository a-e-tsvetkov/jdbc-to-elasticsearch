package com.gmail.a.e.tsvetkov.integrationtest;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.sql.*;

public class SelectSimpleTest extends TestBase {
    private Connection connection;

    @BeforeMethod
    public void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:mydriver:" + host);
    }

    @AfterMethod
    public void tearDown() throws SQLException {
        connection.close();
    }

    @Test
    public void simpleSelect() throws SQLException {
        createSimpleTable();
        execute("insert into table1 (field1, field2) values (1, 2)");
        ResultSet res = executeQuery(
                "select field1, field2 from table1");
        assertResult(res,
                row(i(1), i(2))
        );
    }

    @Test
    public void selectUsingCorrelationName() throws SQLException {
        createSimpleTable();
        execute("insert into table1 (field1, field2) values (1, 2)");
        ResultSet res = executeQuery(
                "select t.field1 from table1 t");
        assertResult(res,
                row(i(1))
        );
    }

    @Test
    public void whereSimpleCondition() throws SQLException {
        createSimpleTable();
        execute("insert into table1 (field1, field2) values (1, 2), (3, 4)");
        ResultSet res = executeQuery(
                "select field1, field2 from table1 where field1 = 1");
        assertResult(res,
                row(i(1), i(2))
        );
    }

    @Test
    public void join() throws SQLException {
        execute("create table table1(key int, value1 int)");
        execute("create table table2(key int, table1_key int, value2 int)");

        execute("insert into table1 (key, value1) values (1, 10)");
        execute("insert into table2 (key, table1_key, value2) values (100, 1, 20)");

        ResultSet res = executeQuery(
                "select table1.key, table2.key, table1.value1, table2.value2 " +
                        "from table1 left join table2 on table1.key = table2.table1_key");
        assertResult(res,
                row(i(1), i(100), i(10), i(20))
        );
    }

    @Test
    public void selectClauseExpression() throws SQLException {
        createSimpleTable();
        execute("insert into table1 (field1, field2) values (1, 2)");
        ResultSet res = executeQuery(
                "select field1 + 1, field2 <> 2 from table1");
        assertResult(res,
                row(i(2), b(false))
        );
    }

    private void execute(String sql) throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute(sql);
    }

    private ResultSet executeQuery(String sql) throws SQLException {
        Statement statement = connection.createStatement();
        return statement.executeQuery(sql);
    }

    private void createSimpleTable() throws SQLException {
        execute("create table table1(field1 int, field2 int)");
    }

}