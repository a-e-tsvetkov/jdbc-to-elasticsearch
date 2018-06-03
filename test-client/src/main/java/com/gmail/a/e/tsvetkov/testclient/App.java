package com.gmail.a.e.tsvetkov.testclient;

import java.sql.*;

public class App {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        Class.forName(com.gmail.a.e.tsvetkov.driver.Driver.class.getName());

        Connection connection = DriverManager.getConnection("jdbc:mydriver:localhost");
        System.out.println("Connection = " + connection);

        Statement statement = connection.createStatement();
        statement.execute("create table t1( field1 int)");

        statement.executeUpdate("insert into  field1 (t1) values ('v1')");

        ResultSet res = statement.executeQuery("select field1 from t1");
        while(res.next()){
            int f1ByIndex = res.getInt(0);
            int f1ByLabel = res.getInt("field1");
            if(f1ByIndex!=f1ByLabel){
                throw new RuntimeException();
            }
        }
    }
}
