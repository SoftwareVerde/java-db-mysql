package com.softwareverde.database.mysql.row;

import com.softwareverde.database.Row;
import com.softwareverde.util.StringUtil;
import com.softwareverde.util.Util;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MysqlRow implements Row {
    public static MysqlRow fromResultSet(final ResultSet resultSet) {
        final MysqlRow mysqlRow = new MysqlRow();

        try {
            final ResultSetMetaData metaData = resultSet.getMetaData();
            for (int i = 0; i < metaData.getColumnCount(); ++i) {
                final String columnName = metaData.getColumnLabel(i + 1).toLowerCase(); // metaData.getColumnName(i+1);
                final String columnValue = resultSet.getString(i + 1);

                mysqlRow._columnNames.add(columnName);
                mysqlRow._columnValues.put(columnName, columnValue);
            }
        } catch (final SQLException exception) { }

        return mysqlRow;
    }

    protected List<String> _columnNames = new ArrayList<String>();
    protected Map<String, String> _columnValues = new HashMap<String, String>();

    protected MysqlRow() { }

    protected String _getString(final String columnName) {
        if (! _columnValues.containsKey(columnName.toLowerCase())) {
            throw new IllegalArgumentException("Row does not contain column: "+ columnName);
        }

        return _columnValues.get(columnName.toLowerCase());
    }

    @Override
    public List<String> getColumnNames() {
        return new ArrayList<String>(_columnNames);
    }

    @Override
    public String getString(final String columnName) {
        return _getString(columnName);
    }

    @Override
    public Integer getInteger(final String columnName) {
        return Util.parseInt(_getString(columnName));
    }

    @Override
    public Long getLong(final String columnName) {
        return Util.parseLong(_getString(columnName));
    }

    @Override
    public Float getFloat(final String columnName) {
        return Util.parseFloat(_getString(columnName));
    }

    @Override
    public Double getDouble(final String columnName) {
        return Util.parseDouble(_getString(columnName));
    }

    @Override
    public Boolean getBoolean(final String columnName) {
        return Util.parseBool(_getString(columnName));
    }

    @Override
    public byte[] getBytes(final String columnName) {
        return StringUtil.stringToBytes(_getString(columnName));
    }
}
