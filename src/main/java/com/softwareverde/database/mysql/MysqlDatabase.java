package com.softwareverde.database.mysql;

import com.softwareverde.database.Database;
import com.softwareverde.database.DatabaseException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class MysqlDatabase implements Database<Connection> {
    private String _username = "root";
    private String _password = "";
    private String _url = "localhost";
    private String _databaseName = "";

    protected Connection _connect() throws SQLException, ClassNotFoundException {
        Class.forName("com.mysql.jdbc.Driver");
        return DriverManager.getConnection("jdbc:mysql://"+ _url +"/"+ _databaseName +"?user="+ _username +"&password="+ _password +"&useSSL=false&serverTimezone=UTC");
    }

    public MysqlDatabase(final String url, final String username, final String password) {
        _url = url;
        _username = username;
        _password = password;
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
