package com.softwareverde.database.mysql;

import com.softwareverde.database.DatabaseConnectionFactory;
import com.softwareverde.database.DatabaseException;
import com.softwareverde.util.Util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class MysqlDatabaseConnectionFactory implements DatabaseConnectionFactory<Connection> {
    public static String createConnectionString(final String hostname, final Integer port, final String schema) {
        final Integer defaultPort = 8336;
        return "jdbc:mysql://" + hostname + ":" + Util.coalesce(port, defaultPort) + "/" + Util.coalesce(schema, "");
    }

    private final String _connectionString;
    private final Properties _properties;

    public MysqlDatabaseConnectionFactory(final String connectionString, final String username, final String password) {
        _connectionString = connectionString;
        _properties = new Properties();

        _properties.setProperty("user", username);
        _properties.setProperty("password", password);
    }

    public MysqlDatabaseConnectionFactory(final String connectionString, final String username, final String password, final Properties properties) {
        _connectionString = connectionString;
        _properties = new Properties(properties);

        _properties.setProperty("user", username);
        _properties.setProperty("password", password);
    }

    @Override
    public MysqlDatabaseConnection newConnection() throws DatabaseException {
        try {
            Class.forName("com.mysql.jdbc.Driver");

            final Connection connection = DriverManager.getConnection(_connectionString, _properties);
            return new MysqlDatabaseConnection(connection);
        }
        catch (final Exception exception) {
            throw new DatabaseException(exception);
        }
    }
}
