package com.softwareverde.database.mysql.row;


import java.sql.ResultSet;

public interface RowFactory {
    MysqlRow fromResultSet(final ResultSet resultSet);
}