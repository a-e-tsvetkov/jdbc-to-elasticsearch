package com.gmail.a.e.tsvetkov.testclient;

import java.sql.*;

public class App {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        Class.forName(com.gmail.a.e.tsvetkov.driver.Driver.class.getName());

        try (Connection connection = DriverManager.getConnection("jdbc:mydriver:localhost")) {
            Statement statement = connection.createStatement();
//            statement.execute("create table t1( field1 int)");

//            statement.executeUpdate("insert into t1 (field1) values (4),(5)");


            ResultSet res = statement.executeQuery("select field1 from t1");
            ResultSetMetaData metaData = res.getMetaData();
            check (metaData.getColumnCount() == 1);
            check (metaData.getColumnType(1) == Types.NUMERIC);
            while (res.next()) {
                String f1ByIndex = res.getString(1);
                String f1ByLabel = res.getString("field1");
                check(f1ByIndex.equals(f1ByLabel));
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
