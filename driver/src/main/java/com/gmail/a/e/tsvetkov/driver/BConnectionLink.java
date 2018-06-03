package com.gmail.a.e.tsvetkov.driver;

import com.gmail.a.e.tsvetkov.driver.sql.parser.SqlParseResult;
import com.gmail.a.e.tsvetkov.driver.sql.parser.SqlParser;
import com.gmail.a.e.tsvetkov.driver.sql.SqlStatement;

import java.sql.SQLException;

public class BConnectionLink {
    public BResultSet execute(String sql) throws SQLException {
        parseSql(sql);

        return null;
    }

    public void close() {

    }

    private SqlStatement parseSql(String sql) throws SQLException {
        SqlParseResult parseResult = SqlParser.parse(sql);
        if (parseResult.isError()) {
            throw new SQLException(parseResult.error());
        } else {
            return parseResult.statement();
        }
    }
}
