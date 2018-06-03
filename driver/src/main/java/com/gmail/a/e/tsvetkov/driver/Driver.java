package com.gmail.a.e.tsvetkov.driver;


import com.gmail.a.e.tsvetkov.driver.connectionstring.parser.ConnectionString;
import com.gmail.a.e.tsvetkov.driver.connectionstring.parser.ConnectionStringParser;

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class Driver implements java.sql.Driver
{

    static {
        try {
            java.sql.DriverManager.registerDriver(new Driver());
        } catch (SQLException E) {
            throw new RuntimeException("Can't register driver!");
        }
    }
    
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        ConnectionString connectionString = ConnectionStringParser.parse(url);
        if(connectionString.isSupported()){
            if(connectionString.isError()){
                throw new SQLException(connectionString.error());
            }else {
                return new AConnection(connectionString.host());
            }
        }else{
            return null;
        }

    }

    @Override
    public boolean acceptsURL(String url) {
        return false;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }
}
