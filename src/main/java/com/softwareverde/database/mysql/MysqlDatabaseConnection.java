package com.softwareverde.database.mysql;

import com.softwareverde.database.DatabaseConnection;
import com.softwareverde.database.DatabaseException;
import com.softwareverde.database.Query;
import com.softwareverde.database.Row;
import com.softwareverde.database.mysql.row.MysqlRowFactory;
import com.softwareverde.database.mysql.row.RowFactory;
import com.softwareverde.util.Util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class MysqlDatabaseConnection implements DatabaseConnection<Connection> {
    private RowFactory _rowFactory = new MysqlRowFactory();
    private Connection _connection;
    private String _lastInsertId = "-1";

    private String _extractInsertId(final PreparedStatement preparedStatement) throws SQLException {
        final ResultSet resultSet = preparedStatement.getGeneratedKeys();

        final Integer insertId;
        {
            if (resultSet.next()) {
                insertId = resultSet.getInt(1);
            }
            else {
                insertId = null;
            }
        }

        resultSet.close();
        return Util.coalesce(insertId, "-1").toString();
    }

    private PreparedStatement _prepareStatement(final String query, final String[] parameters) throws SQLException {
        final PreparedStatement preparedStatement = _connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
        if (parameters != null) {
            for (int i = 0; i < parameters.length; ++i) {
                preparedStatement.setString(i+1, parameters[i]);
            }
        }
        return preparedStatement;
    }

    private void _executeSql(final String query, final String[] parameters) throws SQLException {
        try {
            final PreparedStatement preparedStatement = _prepareStatement(query, parameters);
            preparedStatement.execute();
            _lastInsertId = _extractInsertId(preparedStatement);
            preparedStatement.close();
        }
        catch (final SQLException exception) {
            _lastInsertId = null;
            throw exception;
        }
    }

    public MysqlDatabaseConnection(final Connection connection) {
        _connection = connection;
    }

    @Override
    public synchronized void executeDdl(final String query) throws DatabaseException {
        try {
            if (_connection.isClosed()) {
                throw new DatabaseException("Attempted to execute DDL statement while disconnected.");
            }

            final Statement statement = _connection.createStatement();
            statement.execute(query);
            statement.close();
        }
        catch (final SQLException exception) {
            throw new DatabaseException("Error executing DDL statement.", exception);
        }
    }

    @Override
    public synchronized void executeDdl(final Query query) throws DatabaseException {
        this.executeDdl(query.getQueryString());
    }

    @Override
    public synchronized Long executeSql(final String query, final String[] parameters) throws DatabaseException {
        try {
            if (_connection.isClosed()) {
                throw new DatabaseException("Attempted to execute SQL statement while disconnected.");
            }

            _executeSql(query, parameters);
            return Util.parseLong(_lastInsertId);
        }
        catch (final SQLException exception) {
            throw new DatabaseException("Error executing SQL statement.", exception);
        }
    }

    @Override
    public synchronized Long executeSql(final Query query) throws DatabaseException {
        return this.executeSql(query.getQueryString(), query.getParameters().toArray(new String[0]));
    }

    @Override
    public synchronized List<Row> query(final String query, final String[] parameters) throws DatabaseException {
        try {
            if (_connection.isClosed()) {
                throw new DatabaseException("Attempted to execute query while disconnected.");
            }

            final PreparedStatement preparedStatement = _prepareStatement(query, parameters);
            final ResultSet resultSet = preparedStatement.executeQuery();

            final List<Row> results = new ArrayList<Row>();
            if (resultSet.first()) {
                do {
                    results.add(_rowFactory.fromResultSet(resultSet));
                } while (resultSet.next());
            }
            resultSet.close();
            preparedStatement.close();

            return results;
        }
        catch (final SQLException exception) {
            throw new DatabaseException("Error executing query.", exception);
        }
    }

    @Override
    public synchronized List<Row> query(final Query query) throws DatabaseException {
        return this.query(query.getQueryString(), query.getParameters().toArray(new String[0]));
    }

    @Override
    public Connection getRawConnection() {
        return _connection;
    }

    @Override
    public void close() throws DatabaseException {
        try {
            _connection.close();
        }
        catch (final SQLException exception) {
            throw new DatabaseException("Unable to close database connection.", exception);
        }
    }
}
