package com.softwareverde.database.mysql.properties;

public class MutableDatabaseProperties extends DatabasePropertiesCore implements DatabaseProperties {
    public void setRootPassword(final String rootPassword) { _rootPassword = rootPassword; }
    public void setHostname(final String hostname) { _hostname = hostname; }
    public void setUsername(final String username) { _username = username; }
    public void setPassword(final String password) { _password = password; }
    public void setSchema(final String schema) { _schema = schema; }
    public void setPort(final Integer port) { _port = port; }

    public MutableDatabaseProperties() { }

    public MutableDatabaseProperties(final DatabaseProperties databaseProperties) {
        super(databaseProperties);
    }

    @Override
    public ImmutableDatabaseProperties asConst() {
        return new ImmutableDatabaseProperties(this);
    }
}
