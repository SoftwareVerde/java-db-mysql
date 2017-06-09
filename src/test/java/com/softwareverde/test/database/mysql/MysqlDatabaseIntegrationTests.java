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
        databaseConnection.executeDdl("CREATE TABLE test_table (id int unsigned not null primary key auto_increment, value varchar(255))");
        for (int i=0; i<insertCount; ++i) {
            final Long rowId = databaseConnection.executeSql(new Query("INSERT INTO test_table (value) VALUES (?)").setParameter(i));
            rowIds.add(rowId);
        }
        rows = databaseConnection.query(new Query("SELECT * FROM test_table WHERE value < ?").setParameter(""+ (insertCount / 2)));
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
            final String valueString = row.getString("value");

            final Long idLong = row.getLong("id");
            final Integer valueInt = row.getInteger("value");

            Assert.assertEquals(i+1, idLong.intValue());
            Assert.assertEquals(String.valueOf(i+1), idString);

            Assert.assertEquals(i, valueInt.intValue());
            Assert.assertEquals(String.valueOf(i), valueString);
        }
    }
}
