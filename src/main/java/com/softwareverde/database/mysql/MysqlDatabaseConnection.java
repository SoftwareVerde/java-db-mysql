package com.softwareverde.database.mysql;

import com.softwareverde.database.DatabaseConnection;
import com.softwareverde.database.DatabaseException;
import com.softwareverde.database.Query;
import com.softwareverde.database.Row;
import com.softwareverde.database.mysql.row.MysqlRowFactory;
import com.softwareverde.database.mysql.row.RowFactory;
import com.softwareverde.database.query.parameter.ParameterType;
import com.softwareverde.database.query.parameter.TypedParameter;
import com.softwareverde.util.Util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class MysqlDatabaseConnection implements DatabaseConnection<Connection>, AutoCloseable {
    private static final Long INVALID_ID = -1L;
    private static final Integer INVALID_ROW_COUNT = -1;

    private RowFactory _rowFactory = new MysqlRowFactory();
    private Connection _connection;
    private Long _lastInsertId = INVALID_ID;
    private Integer _lastRowAffectedCount = INVALID_ROW_COUNT;

    private TypedParameter[] _stringArrayToTypedParameters(final String[] parameters) {
        if (parameters == null) { return new TypedParameter[0]; }

        final TypedParameter[] typedParameters = new TypedParameter[parameters.length];
        for (int i=0; i<parameters.length; ++i) {
            typedParameters[i] = new TypedParameter(parameters[i], ParameterType.STRING);
        }
        return typedParameters;
    }

    private Long _extractInsertId(final PreparedStatement preparedStatement) throws SQLException {
        try (final ResultSet resultSet = preparedStatement.getGeneratedKeys()) {
            final Long insertId;
            {
                if (resultSet.next()) {
                    insertId = resultSet.getLong(1);
                }
                else {
                    insertId = null;
                }
            }
            return Util.coalesce(insertId, INVALID_ID);
        }
    }

    private PreparedStatement _prepareStatement(final String query, final TypedParameter[] parameters) throws SQLException {
        final PreparedStatement preparedStatement = _connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
        if (parameters != null) {
            for (int i = 0; i < parameters.length; ++i) {
                final Integer parameterIndex = (i + 1);
                final TypedParameter typedParameter = parameters[i];
                switch (typedParameter.type) {
                    case BYTE_ARRAY: {
                        preparedStatement.setBytes(parameterIndex, (byte[]) typedParameter.value);
                    } break;
                    case BOOLEAN: {
                        preparedStatement.setBoolean(parameterIndex, (Boolean) typedParameter.value);
                    } break;
                    case STRING: {
                        preparedStatement.setString(parameterIndex, (String) typedParameter.value);
                    } break;
                    default: {
                        final String stringValue = (typedParameter.value == null ? null : typedParameter.value.toString());
                        preparedStatement.setString(parameterIndex, stringValue);
                    } break;
                }
            }
        }
        return preparedStatement;
    }

    private void _executeAsPreparedStatement(final String query, final TypedParameter[] typedParameters) throws SQLException {
        try (final PreparedStatement preparedStatement = _prepareStatement(query, typedParameters)) {
            preparedStatement.execute();
            _lastInsertId = _extractInsertId(preparedStatement);
            _lastRowAffectedCount = preparedStatement.getUpdateCount();
        }
        catch (final SQLException exception) {
            _lastInsertId = INVALID_ID;
            _lastRowAffectedCount = INVALID_ROW_COUNT;
            throw exception;
        }
    }

    private Long _executeSql(final String query, final TypedParameter[] parameters) throws DatabaseException {
        try {
            if (_connection.isClosed()) {
                throw new DatabaseException("Attempted to execute SQL statement while disconnected.");
            }

            _executeAsPreparedStatement(query, parameters);
            return _lastInsertId;
        }
        catch (final SQLException exception) {
            throw new DatabaseException("Error executing SQL statement.", exception);
        }
    }

    private List<Row> _query(final String query, final TypedParameter[] typedParameters) throws DatabaseException {
        try {
            if (_connection.isClosed()) {
                throw new DatabaseException("Attempted to execute query while disconnected.");
            }

            final List<Row> results = new ArrayList<Row>();
            try (final PreparedStatement preparedStatement = _prepareStatement(query, typedParameters);
                 final ResultSet resultSet = preparedStatement.executeQuery() ) {

                if (resultSet.first()) {
                    do {
                        results.add(_rowFactory.fromResultSet(resultSet));
                    } while (resultSet.next());
                }
            }
            return results;
        }
        catch (final SQLException exception) {
            throw new DatabaseException("Error executing query.", exception);
        }
    }

    public MysqlDatabaseConnection(final Connection connection) {
        _connection = connection;
    }

    public Integer getRowsAffectedCount() {
        return _lastRowAffectedCount;
    }

    @Override
    public synchronized void executeDdl(final String query) throws DatabaseException {
        try {
            if (_connection.isClosed()) {
                throw new DatabaseException("Attempted to execute DDL statement while disconnected.");
            }

            try (final Statement statement = _connection.createStatement()) {
                statement.execute(query);
            }
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
        final TypedParameter[] typedParameters = _stringArrayToTypedParameters(parameters);
        return _executeSql(query, typedParameters);
    }

    @Override
    public synchronized Long executeSql(final Query query) throws DatabaseException {
        return _executeSql(query.getQueryString(), query.getParameters().toArray(new TypedParameter[0]));
    }

    @Override
    public synchronized List<Row> query(final String query, final String[] parameters) throws DatabaseException {
        final TypedParameter[] typedParameters = _stringArrayToTypedParameters(parameters);
        return _query(query, typedParameters);
    }

    @Override
    public synchronized List<Row> query(final Query query) throws DatabaseException {
        return _query(query.getQueryString(), query.getParameters().toArray(new TypedParameter[0]));
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
