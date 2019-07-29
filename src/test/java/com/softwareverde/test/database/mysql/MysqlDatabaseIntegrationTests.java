package com.softwareverde.test.database.mysql;

import ch.vorburger.exec.ManagedProcessException;
import ch.vorburger.mariadb4j.DB;
import ch.vorburger.mariadb4j.DBConfigurationBuilder;
import com.softwareverde.database.Database;
import com.softwareverde.database.mysql.MysqlDatabaseConnection;
import com.softwareverde.database.query.Query;
import com.softwareverde.database.row.Row;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;

public class MysqlDatabaseIntegrationTests {
    protected static class EmbeddedMariaDbDatabase implements Database<Connection> {
        public final String schema = "test_database_"+ ((int) (Math.random() * Integer.MAX_VALUE));

        protected final DBConfigurationBuilder _dbConfigurationBuilder;
        protected final DB _db;

        public EmbeddedMariaDbDatabase() throws ManagedProcessException {
            _dbConfigurationBuilder = DBConfigurationBuilder.newBuilder();
            _db = DB.newEmbeddedDB(_dbConfigurationBuilder.build());
            _db.start();
            _db.createDB(this.schema);
        }

        @Override
        public MysqlDatabaseConnection newConnection() {
            try {
                Class.forName("com.mysql.jdbc.Driver");
                final Connection connection = DriverManager.getConnection(_dbConfigurationBuilder.getURL(this.schema));
                return new MysqlDatabaseConnection(connection);
            }
            catch (final Exception exception) {
                throw new RuntimeException(exception);
            }
        }
    }

    protected static final EmbeddedMariaDbDatabase _database;
    static {
        try {
            _database = new EmbeddedMariaDbDatabase();
        }
        catch (final Exception exception) {
            throw new RuntimeException(exception);
        }
    }

    @Test
    public void database_should_query_real_local_instance() throws Exception {
        // Setup
        final MysqlDatabaseConnection databaseConnection = _database.newConnection();
        final int insertCount = 10;
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
        Assert.assertEquals(insertCount, rowIds.size());
        for (int i = 0; i < insertCount; ++i) {
            final Long rowId = rowIds.get(i);
            Assert.assertEquals(i + 1, rowId.intValue());
        }

        Assert.assertEquals(insertCount / 2, rows.size());
        for (int i = 0; i < (insertCount / 2); ++i) {
            final Row row = rows.get(i);

            final String idString = row.getString("id");
            Assert.assertEquals(String.valueOf(i + 1), idString);

            final String valueString = row.getString("value");
            Assert.assertEquals(String.valueOf(i), valueString);

            final Long idLong = row.getLong("id");
            Assert.assertEquals(i + 1, idLong.intValue());

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
        final MysqlDatabaseConnection databaseConnection = _database.newConnection();

        final byte[] bytes = new byte[256];
        for (int i = 0; i < bytes.length; ++i) {
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
        for (int i = 0; i < receivedBytes.length; ++i) {
            Assert.assertEquals(bytes[i], receivedBytes[i]);
        }
    }

    @Test
    public void database_should_get_and_store_null_blob() throws Exception {
        // Setup
        final MysqlDatabaseConnection databaseConnection = _database.newConnection();

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

    @Test
    public void database_should_return_number_of_affected_rows() throws Exception {
        final MysqlDatabaseConnection databaseConnection = _database.newConnection();
        databaseConnection.executeDdl("DROP TABLE IF EXISTS test_table");
        databaseConnection.executeDdl("CREATE TABLE test_table (id int unsigned not null primary key auto_increment, `key` varchar(255) not null, value varchar(255) not null, UNIQUE KEY key_uq (`key`))");

        {
            databaseConnection.executeSql(
                new Query("INSERT INTO test_table (`key`, value) VALUES ('1', '1')")
            );
            Assert.assertEquals(1, databaseConnection.getRowsAffectedCount().intValue());
        }

        {
            final Long rowId = databaseConnection.executeSql(
                new Query("INSERT IGNORE INTO test_table (`key`, value) VALUES ('1', '1'), ('2', '2')") // Contains one duplicate, one unique...
            );
            Assert.assertEquals(2L, rowId.longValue());
            Assert.assertEquals(1, databaseConnection.getRowsAffectedCount().intValue());
        }

        {
            final Long rowId = databaseConnection.executeSql(
                new Query("INSERT IGNORE INTO test_table (`key`, value) VALUES ('2', '2'), ('3', '3'), ('4', '4')") // Contains one duplicate, two unique...
            );
            Assert.assertEquals(2, databaseConnection.getRowsAffectedCount().intValue());

            final Long expectedValue = databaseConnection.query("SELECT id FROM test_table WHERE `key` = '3'", null).get(0).getLong("id");
            Assert.assertEquals(expectedValue, rowId);
        }

        databaseConnection.close();
    }
}
