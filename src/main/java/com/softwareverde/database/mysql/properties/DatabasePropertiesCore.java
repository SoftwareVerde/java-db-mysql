package com.softwareverde.database.mysql.properties;

public abstract class DatabasePropertiesCore implements DatabaseProperties {
    protected String _rootPassword;
    protected String _hostname;
    protected String _username;
    protected String _password;
    protected String _schema;
    protected Integer _port;

    protected DatabasePropertiesCore() { }

    protected DatabasePropertiesCore(final DatabaseProperties databaseProperties) {
        _rootPassword = databaseProperties.getRootPassword();
        _hostname = databaseProperties.getHostname();
        _username = databaseProperties.getUsername();
        _password = databaseProperties.getPassword();
        _schema = databaseProperties.getSchema();
        _port = databaseProperties.getPort();
    }

    @Override
    public String getRootPassword() { return  _rootPassword; }

    @Override
    public String getHostname() { return _hostname; }

    @Override
    public String getUsername() { return _username; }

    @Override
    public String getPassword() { return _password; }

    @Override
    public String getSchema() { return _schema; }

    @Override
    public Integer getPort() { return _port; }

    @Override
    public Credentials getRootCredentials() {
        return new Credentials("root", _rootPassword);
    }

    @Override
    public Credentials getCredentials() {
        return new Credentials(_username, _password);
    }
}
