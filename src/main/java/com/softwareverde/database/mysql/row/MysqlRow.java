package com.softwareverde.database.mysql.row;

import com.softwareverde.database.jdbc.row.JdbcRow;

public class MysqlRow extends JdbcRow {

    protected MysqlRow(final JdbcRow jdbcRow) {
        super(jdbcRow);
    }

    public MysqlRow() { }
}
