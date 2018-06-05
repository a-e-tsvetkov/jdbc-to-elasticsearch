package com.gmail.a.e.tsvetkov.driver;

import com.gmail.a.e.tsvetkov.driver.resultset.AResultSet;
import com.gmail.a.e.tsvetkov.driver.sql.SqlStatement;
import com.gmail.a.e.tsvetkov.driver.sql.executor.SqlExecutor;
import com.gmail.a.e.tsvetkov.driver.sql.parser.SqlParseResult;
import com.gmail.a.e.tsvetkov.driver.sql.parser.SqlParser;
import com.sksamuel.elastic4s.http.HttpClient;

import java.sql.SQLException;

public class BConnectionLink {
    private final HttpClient client;

    private BConnectionLink(HttpClient client) {
        this.client = client;
    }

    public static BConnectionLink connect(String host) {
        HttpClient connect = SqlExecutor.connect(host);
        return new BConnectionLink(connect);
    }

    public AResultSet execute(String sql) throws SQLException {
        var statement = parseSql(sql);

        return SqlExecutor.execute(client, statement);
    }

    public void close() {
        client.close();
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
