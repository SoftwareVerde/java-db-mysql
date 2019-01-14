package com.softwareverde.database.mysql.properties;

import com.softwareverde.constable.Constable;

public interface DatabaseProperties extends Constable<ImmutableDatabaseProperties> {
    String getRootPassword();
    String getHostname();
    String getUsername();
    String getPassword();
    String getSchema();
    Integer getPort();

    Credentials getRootCredentials();
    Credentials getCredentials();

    ImmutableDatabaseProperties asConst();
}
