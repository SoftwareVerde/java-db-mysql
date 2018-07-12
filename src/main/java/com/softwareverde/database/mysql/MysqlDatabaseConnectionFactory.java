package com.softwareverde.database.mysql;

import com.softwareverde.database.DatabaseConnectionFactory;
import com.softwareverde.database.DatabaseException;
import com.softwareverde.util.Util;

import java.sql.Connection;
import java.sql.DriverManager;

public class MysqlDatabaseConnectionFactory implements DatabaseConnectionFactory<Connection> {
    public static String createConnectionString(final String hostname, final Integer port, final String schema) {
        final Integer defaultPort = 8336;
        return "jdbc:mysql://" + hostname + ":" + Util.coalesce(port, defaultPort) + "/" + Util.coalesce(schema, "");
    }

    private final String _connectionString;
    private final String _username;
    private final String _password;

    public MysqlDatabaseConnectionFactory(final String connectionString, final String username, final String password) {
        _connectionString = connectionString;
        _username = username;
        _password = password;
    }

    @Override
    public MysqlDatabaseConnection newConnection() throws DatabaseException {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            final Connection connection = DriverManager.getConnection(_connectionString, _username, _password);
            return new MysqlDatabaseConnection(connection);
        }
        catch (final Exception exception) {
            throw new DatabaseException(exception);
        }
    }
}
