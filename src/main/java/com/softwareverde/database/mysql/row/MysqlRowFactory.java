package com.softwareverde.database.mysql.row;

import com.softwareverde.database.DatabaseException;
import com.softwareverde.database.jdbc.row.JdbcRow;
import com.softwareverde.database.jdbc.row.JdbcRowFactory;

import java.sql.ResultSet;

public class MysqlRowFactory extends JdbcRowFactory {
    @Override
    public MysqlRow fromResultSet(final ResultSet resultSet) throws DatabaseException {
        final JdbcRow jdbcRow = super.fromResultSet(resultSet);
        return new MysqlRow(jdbcRow);
    }
}