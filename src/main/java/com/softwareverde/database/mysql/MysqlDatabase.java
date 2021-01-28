package com.softwareverde.database.mysql;

import com.softwareverde.database.jdbc.JdbcDatabase;
import com.softwareverde.database.properties.DatabaseCredentials;
import com.softwareverde.database.properties.DatabaseProperties;

import java.util.Properties;

public class MysqlDatabase extends MysqlDatabaseConnectionFactory implements JdbcDatabase {
    public static final Integer DEFAULT_PORT = 3306;

    public MysqlDatabase(final DatabaseProperties databaseProperties) {
        this(databaseProperties, databaseProperties.getCredentials());
    }

    public MysqlDatabase(final DatabaseProperties databaseProperties, final DatabaseCredentials credentials) {
        super(databaseProperties.getHostname(), databaseProperties.getPort(), databaseProperties.getSchema(), credentials.username, credentials.password);
    }

    public MysqlDatabase(final String host, final Integer port, final String username, final String password) {
        super(host, port, "", username, password);
    }

    public MysqlDatabase(final String host, final Integer port, final String username, final String password, final Properties properties) {
        super(host, port, "", username, password, properties);
    }

    public void setSchema(final String schema) {
        _schema = schema;
    }

    public MysqlDatabaseConnectionFactory newConnectionFactory() {
        return new MysqlDatabaseConnectionFactory(_hostname, _port, _schema, _username, _password, _connectionProperties);
    }
}
