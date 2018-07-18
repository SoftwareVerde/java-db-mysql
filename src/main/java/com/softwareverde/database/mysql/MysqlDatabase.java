package com.softwareverde.database.mysql;

import com.softwareverde.database.Database;
import com.softwareverde.database.DatabaseException;
import com.softwareverde.util.Util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class MysqlDatabase implements Database<Connection> {
    private final String _url;
    private final Properties _properties;
    private String _databaseName = "";

    protected Connection _connect() throws SQLException, ClassNotFoundException {
        Class.forName("com.mysql.jdbc.Driver");
        return DriverManager.getConnection("jdbc:mysql://"+ _url +"/"+ Util.coalesce(_databaseName, ""), _properties);
    }

    public MysqlDatabase(final String url, final String username, final String password) {
        _url = url;
        _properties = new Properties();

        _properties.setProperty("user", username);
        _properties.setProperty("password", password);
        _properties.setProperty("useSSL", "false");
        _properties.setProperty("serverTimezone", "UTC");
    }

    public MysqlDatabase(final String url, final String username, final String password, final Properties properties) {
        _url = url;
        _properties = new Properties(properties);

        _properties.setProperty("user", username);
        _properties.setProperty("password", password);
        _properties.setProperty("useSSL", "false");
        _properties.setProperty("serverTimezone", "UTC");
    }

    public void setDatabase(final String databaseName) throws DatabaseException {
        _databaseName = databaseName;
    }

    @Override
    public MysqlDatabaseConnection newConnection() throws DatabaseException {
        try {
            final Connection connection = _connect();
            return new MysqlDatabaseConnection(connection);
        }
        catch (SQLException | ClassNotFoundException exception) {
            throw new DatabaseException("Unable to connect to database.", exception);
        }
    }

    /**
     * Require dependencies be packaged at compile-time.
     */
    private static final Class[] UNUSED = {
        com.mysql.jdbc.Driver.class
    };
}
