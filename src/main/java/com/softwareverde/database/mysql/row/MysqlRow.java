package com.softwareverde.database.mysql.row;

import com.softwareverde.database.Row;
import com.softwareverde.util.Util;

import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MysqlRow implements Row {
    public static final String STRING_ENCODING = "ISO-8859-1";

    protected static final Charset CHARSET;
    static {
        Charset charset;
        try {
            charset = Charset.forName(STRING_ENCODING);
        }
        catch (final Exception exception) {
            charset = Charset.defaultCharset();
        }
        CHARSET = charset;
    }

    protected static Boolean _isBinaryType(final Integer sqlDataType) {
        switch (sqlDataType) {
            case Types.BLOB:
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY: {
                return true;
            }
            default: { return false; }
        }
    }

    public static MysqlRow fromResultSet(final ResultSet resultSet) {
        final MysqlRow mysqlRow = new MysqlRow();

        try {
            final ResultSetMetaData metaData = resultSet.getMetaData();
            for (int i = 0; i < metaData.getColumnCount(); ++i) {
                final Integer columnIndex = (i + 1);
                final Integer sqlDataType = metaData.getColumnType(columnIndex);
                final Boolean isBinaryType = _isBinaryType(sqlDataType);

                final String columnName = metaData.getColumnLabel(columnIndex).toLowerCase();
                final String columnValue;
                {
                    String stringValue;
                    if (isBinaryType) {
                        final byte[] bytes = resultSet.getBytes(columnIndex);
                        stringValue = (bytes != null ? new String(bytes, CHARSET) : null);
                    }
                    else {
                        stringValue = resultSet.getString(columnIndex);
                    }
                    columnValue = stringValue;
                }

                mysqlRow._columnNames.add(columnName);
                mysqlRow._columnValues.put(columnName, columnValue);
            }
        }
        catch (final SQLException exception) { }

        return mysqlRow;
    }

    protected List<String> _columnNames = new ArrayList<String>();
    protected Map<String, String> _columnValues = new HashMap<String, String>();

    protected MysqlRow() { }

    protected String _getString(final String columnName) {
        final String lowerCaseColumnName = columnName.toLowerCase();
        if (! _columnValues.containsKey(lowerCaseColumnName)) {
            throw new IllegalArgumentException("Row does not contain column: "+ columnName);
        }

        return _columnValues.get(lowerCaseColumnName);
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
        final String bytesString = _getString(columnName);
        if (bytesString == null) { return null; }
        return bytesString.getBytes(CHARSET);
    }
}
