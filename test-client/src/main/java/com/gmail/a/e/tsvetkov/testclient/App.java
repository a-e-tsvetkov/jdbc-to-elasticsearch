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

            statement.executeUpdate("insert into t1 (field1, field2) values (4, 1),(4, 3),(5, 1)");


            ResultSet res = statement.executeQuery(
                    "select field1 as t, field1 + 1, field2 from t1 where field1=4 and field2>1 and field2 - 3 = 0  ");
            ResultSetMetaData metaData = res.getMetaData();
            check(metaData.getColumnCount() == 3);
            check(metaData.getColumnType(1) == Types.NUMERIC);
            while (res.next()) {
                int f1ByIndex = res.getInt(1);
                int f1ByLabel = res.getInt("t");
                check(f1ByIndex == f1ByLabel);
                int f2 = res.getInt(2);
                int f3 = res.getInt(3);
                System.out.printf("%1d %2d %3d\n", f1ByIndex, f2, f3);
            }

        }
    }

    private static void check(boolean b) {
        if (!b) {
            throw new RuntimeException();
        }
    }
}
