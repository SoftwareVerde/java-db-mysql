package com.softwareverde.database.mysql.properties;

import com.softwareverde.constable.Const;

public class ImmutableDatabaseProperties extends DatabasePropertiesCore implements Const {

    public ImmutableDatabaseProperties(final DatabaseProperties databaseProperties) {
        super(databaseProperties);
    }

    @Override
    public ImmutableDatabaseProperties asConst() {
        return this;
    }
}
