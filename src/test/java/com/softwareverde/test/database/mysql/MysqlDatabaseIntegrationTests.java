package com.softwareverde.test.database.mysql;

import com.softwareverde.database.DatabaseConnection;
import com.softwareverde.database.Query;
import com.softwareverde.database.Row;
import com.softwareverde.database.mysql.MysqlDatabase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

public class MysqlDatabaseIntegrationTests {

    /**
     * Replace the connection-parameters here for your local test instance.
     *  WARNING: The tests in this file will create a random database and drop it at the conclusion.
     */
    protected final String _databaseUrl = "localhost";
    protected final String _username = "root";
    protected final String _password = "";
    protected final String _schema = "test_database_"+ ((int) (Math.random() * Integer.MAX_VALUE));

    protected final MysqlDatabase _database = new MysqlDatabase(_databaseUrl, _username, _password);

    @Before
    public void setUp() throws Exception {
        final DatabaseConnection<Connection> databaseConnection = _database.newConnection();

        databaseConnection.executeDdl("DROP DATABASE IF EXISTS "+ _schema);
        databaseConnection.executeDdl("CREATE DATABASE "+ _schema);

        _database.setDatabase(_schema);

        databaseConnection.close();
    }

    @After
    public void tearDown() throws Exception {
        final DatabaseConnection<Connection> databaseConnection = _database.newConnection();

        databaseConnection.executeDdl("DROP DATABASE IF EXISTS " + _schema);

        databaseConnection.close();
    }

    @Test
    public void database_should_query_real_local_instance() throws Exception {
        // Setup
        final DatabaseConnection<Connection> databaseConnection = _database.newConnection();
        final Integer insertCount = 10;
        final List<Long> rowIds = new ArrayList<Long>();
        final List<Row> rows;

        // Action
        databaseConnection.executeDdl("DROP TABLE IF EXISTS test_table");
        databaseConnection.executeDdl("CREATE TABLE test_table (id int unsigned not null primary key auto_increment, value varchar(255), value_int int, value_int2 int NOT NULL, bytes blob, is_value tinyint(1) NOT NULL)");
        for (int i=0; i<insertCount; ++i) {
            final Long rowId = databaseConnection.executeSql(
                new Query("INSERT INTO test_table (value, value_int, value_int2, bytes, is_value) VALUES (?, ?, ?, ?, ?)")
                .setParameter(i)
                .setParameter(i)
                .setParameter(String.valueOf(i)) // Wrong type should be coerced...
                .setParameter(new byte[]{ 0x00, 0x01, 0x02, 0x03 })
                .setParameter(true)
            );
            rowIds.add(rowId);
        }
        rows = databaseConnection.query(new Query("SELECT * FROM test_table WHERE value < ? ORDER BY id ASC").setParameter(""+ (insertCount / 2)));
        databaseConnection.close();

        // Assert
        Assert.assertEquals(insertCount.intValue(), rowIds.size());
        for (int i=0; i<insertCount; ++i) {
            final Long rowId = rowIds.get(i);
            Assert.assertEquals(i+1, rowId.intValue());
        }

        Assert.assertEquals(insertCount / 2, rows.size());
        for (int i=0; i<insertCount/2; ++i) {
            final Row row = rows.get(i);

            final String idString = row.getString("id");
            Assert.assertEquals(String.valueOf(i+1), idString);

            final String valueString = row.getString("value");
            Assert.assertEquals(String.valueOf(i), valueString);

            final Long idLong = row.getLong("id");
            Assert.assertEquals(i+1, idLong.intValue());

            final Integer valueInt = row.getInteger("value");
            Assert.assertEquals(i, valueInt.intValue());

            final Integer valueColInt = row.getInteger("value_int");
            Assert.assertEquals(i, valueInt.intValue());

            final Integer valueColInt2 = row.getInteger("value_int2");
            Assert.assertEquals(i, valueInt.intValue());

            final byte[] bytes = row.getBytes("bytes");
            Assert.assertEquals(4, bytes.length);
            Assert.assertEquals((byte) 0x00, bytes[0]);
            Assert.assertEquals((byte) 0x01, bytes[1]);
            Assert.assertEquals((byte) 0x02, bytes[2]);
            Assert.assertEquals((byte) 0x03, bytes[3]);

            final Boolean truthy = row.getBoolean("is_value");
            Assert.assertTrue(truthy);
        }
    }

    @Test
    public void database_should_get_and_store_blob() throws Exception {
        // Setup
        final DatabaseConnection<Connection> databaseConnection = _database.newConnection();

        final byte[] bytes = new byte[256];
        for (int i=0; i<bytes.length; ++i) {
            bytes[i] = (byte) i;
        }

        // Action
        databaseConnection.executeDdl("DROP TABLE IF EXISTS test_table");
        databaseConnection.executeDdl("CREATE TABLE test_table (id int unsigned not null primary key auto_increment, value blob)");
        final Long rowId = databaseConnection.executeSql(
            new Query("INSERT INTO test_table (value) VALUES (?)")
                .setParameter(bytes)
        );
        final List<Row> rows = databaseConnection.query(new Query("SELECT * FROM test_table"));
        databaseConnection.close();

        // Assert
        Assert.assertEquals(1, rows.size());
        final Row row = rows.get(0);

        final byte[] receivedBytes = row.getBytes("value");
        Assert.assertEquals(256, bytes.length);
        for (int i=0; i<receivedBytes.length; ++i) {
            Assert.assertEquals(bytes[i], receivedBytes[i]);
        }
    }

    @Test
    public void database_should_get_and_store_null_blob() throws Exception {
        // Setup
        final DatabaseConnection<Connection> databaseConnection = _database.newConnection();

        final byte[] bytes = null;

        // Action
        databaseConnection.executeDdl("DROP TABLE IF EXISTS test_table");
        databaseConnection.executeDdl("CREATE TABLE test_table (id int unsigned not null primary key auto_increment, value blob)");
        final Long rowId = databaseConnection.executeSql(
            new Query("INSERT INTO test_table (value) VALUES (?)")
                .setParameter(bytes)
        );
        final List<Row> rows = databaseConnection.query(new Query("SELECT * FROM test_table"));
        databaseConnection.close();

        // Assert
        Assert.assertEquals(1, rows.size());
        final Row row = rows.get(0);

        final byte[] receivedBytes = row.getBytes("value");
        Assert.assertNull(receivedBytes);
    }
}
