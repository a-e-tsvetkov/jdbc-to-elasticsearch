package com.gmail.a.e.tsvetkov.testclient;

import lombok.extern.java.Log;

import java.sql.*;

@Log
public class App {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        Class.forName(com.gmail.a.e.tsvetkov.driver.Driver.class.getName());

        try (Connection connection = DriverManager.getConnection("jdbc:mydriver:localhost")) {
            Statement statement = connection.createStatement();
            statement.execute("create table t1(field1 int, field2 int)");

            statement.executeUpdate("insert into t1 (field1, field2) values (4, 1),(5, 1)");


            ResultSet res = statement.executeQuery(
                    "select field1, field1 as t from t1");
            ResultSetMetaData metaData = res.getMetaData();
            check(metaData.getColumnCount() == 2);
            check(metaData.getColumnType(1) == Types.NUMERIC);
            while (res.next()) {
                int f1ByIndex = res.getInt(1);
                int f1ByLabel = res.getInt("t");
                check(f1ByIndex == f1ByLabel);
                System.out.println(f1ByIndex);
            }

        }
    }

    private static void check(boolean b) {
        if (!b) {
            throw new RuntimeException();
        }
    }
}
