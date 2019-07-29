package com.softwareverde.database.mysql;

import com.softwareverde.database.connection.JdbcDatabaseConnection;
import com.softwareverde.database.mysql.row.MysqlRowFactory;

import java.sql.Connection;

public class MysqlDatabaseConnection extends JdbcDatabaseConnection {

    protected MysqlDatabaseConnection(final Connection connection, final MysqlRowFactory rowFactory) {
        super(connection, rowFactory);
    }

    public MysqlDatabaseConnection(final Connection connection) {
        super(connection, new MysqlRowFactory());
    }
}
