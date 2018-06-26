package com.gmail.a.e.tsvetkov.integrationtest;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
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
    public void testMain() throws SQLException {

        createSimpleTable();

        Statement statement = connection.createStatement();
        statement.execute("insert into table1 (field1, field2) values (1, 2)");

        ResultSet res = statement.executeQuery(
                "select field1, field2 from table1 where field1=1");

        assertResult(res,
                row(i(1), i(2))
        );
    }

    private void createSimpleTable() throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("create table table1(field1 int, field2 int)");
    }

}