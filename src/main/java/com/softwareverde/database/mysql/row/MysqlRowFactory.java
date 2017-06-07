package com.softwareverde.database.mysql.row;

import java.sql.ResultSet;

public class MysqlRowFactory implements RowFactory {
    public MysqlRow fromResultSet(final ResultSet resultSet) {
        return MysqlRow.fromResultSet(resultSet);
    }
}