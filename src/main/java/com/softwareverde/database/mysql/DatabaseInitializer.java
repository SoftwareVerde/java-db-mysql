package com.softwareverde.database.mysql;

import com.softwareverde.database.DatabaseException;
import com.softwareverde.database.Query;
import com.softwareverde.database.Row;
import com.softwareverde.database.mysql.properties.Credentials;
import com.softwareverde.database.mysql.properties.DatabaseProperties;
import com.softwareverde.util.HashUtil;
import com.softwareverde.util.IoUtil;

import java.io.InputStream;
import java.io.StringReader;
import java.util.List;

public class DatabaseInitializer {
    public interface DatabaseUpgradeHandler {
        Boolean onUpgrade(int previousVersion, int requiredVersion);
    }

    protected final String _initSqlFileName;
    protected final Integer _requiredDatabaseVersion;
    protected final DatabaseUpgradeHandler _databaseUpgradeHandler;

    protected String _getResource(final String resourceFile) {
        final ClassLoader classLoader = this.getClass().getClassLoader();
        final InputStream resourceStream = classLoader.getResourceAsStream(resourceFile);
        if (resourceStream == null) { return null; }
        return IoUtil.streamToString(resourceStream);
    }

    protected Integer _getDatabaseVersionNumber(final MysqlDatabaseConnection databaseConnection) {
        try {
            final List<Row> rows = databaseConnection.query("SELECT version FROM metadata ORDER BY id DESC LIMIT 1", null);
            if (! rows.isEmpty()) {
                final Row row = rows.get(0);
                return row.getInteger("version");
            }
        }
        catch (final Exception exception) { }
        return 0;
    }

    protected void _runSqlScript(final String databaseInitFileContents, final MysqlDatabaseConnection databaseConnection) throws DatabaseException {
        try {
            final ScriptRunner scriptRunner = new ScriptRunner(databaseConnection.getRawConnection(), true, false);
            scriptRunner.runScript(new StringReader(databaseInitFileContents));
        }
        catch (final Exception exception) {
            throw new DatabaseException(exception);
        }
    }

    public DatabaseInitializer() {
        _initSqlFileName = null;
        _requiredDatabaseVersion = 1;
        _databaseUpgradeHandler = new DatabaseUpgradeHandler() {
            @Override
            public Boolean onUpgrade(final int previousVersion, final int requiredVersion) {
                throw new RuntimeException("Database upgrade not supported.");
            }
        };
    }

    public DatabaseInitializer(final String databaseInitFileName, final Integer requiredDatabaseVersion, final DatabaseUpgradeHandler databaseUpgradeHandler) {
        if (databaseInitFileName != null) {
            _initSqlFileName = ((databaseInitFileName.startsWith("/") ? "" : "/") + databaseInitFileName);
        }
        else {
            _initSqlFileName = null;
        }

        _requiredDatabaseVersion = requiredDatabaseVersion;
        _databaseUpgradeHandler = databaseUpgradeHandler;
    }

    /**
     * Creates the schema if it does not exist and a maintenance user to use instead of root.
     *  The maintenance username is [schema]_maintenance; its password being the sha256 hash of the root password.
     *  Returns the maintenance credentials created by this call.
     */
    public void initializeSchema(final MysqlDatabaseConnection rootDatabaseConnection, final DatabaseProperties databaseProperties) throws DatabaseException {
        final Credentials credentials;
        final Credentials maintenanceCredentials;
        {
            final String databaseSchema = databaseProperties.getSchema();
            final String newRootPassword = databaseProperties.getRootPassword();
            final String maintenanceUsername = (databaseSchema + "_maintenance");
            final String maintenancePassword = HashUtil.sha256(newRootPassword);

            credentials = new Credentials(databaseProperties.getUsername(), databaseProperties.getPassword());
            maintenanceCredentials = new Credentials(maintenanceUsername, maintenancePassword);
        }

        try {
            rootDatabaseConnection.executeDdl("CREATE DATABASE IF NOT EXISTS `" + databaseProperties.getSchema() + "`");

            { // Create maintenance user and permissions...
                rootDatabaseConnection.executeSql(
                    new Query("CREATE USER ? IDENTIFIED BY ?")
                        .setParameter(maintenanceCredentials.username)
                        .setParameter(maintenanceCredentials.password)
                );
                rootDatabaseConnection.executeSql(
                    new Query("GRANT ALL PRIVILEGES ON `" + databaseProperties.getSchema() + "`.* TO ?")
                        .setParameter(maintenanceCredentials.username)
                );
            }

            { // Create regular user and permissions...
                rootDatabaseConnection.executeSql(
                    new Query("CREATE USER ? IDENTIFIED BY ?")
                        .setParameter(credentials.username)
                        .setParameter(credentials.password)
                );
                rootDatabaseConnection.executeSql(
                    new Query("GRANT SELECT, INSERT, DELETE, UPDATE, EXECUTE ON `" + databaseProperties.getSchema() + "`.* TO ?")
                        .setParameter(credentials.username)
                );
            }

            rootDatabaseConnection.executeSql("FLUSH PRIVILEGES", null);
        }
        catch (final Exception exception) {
            if (exception instanceof DatabaseException) { throw exception; }
            throw new DatabaseException(exception);
        }
    }

    public Credentials getMaintenanceCredentials(final DatabaseProperties databaseProperties) {
        final String databaseSchema = databaseProperties.getSchema();
        final String rootPassword = databaseProperties.getRootPassword();
        final String maintenanceUsername = (databaseSchema + "_maintenance");
        final String maintenancePassword = HashUtil.sha256(rootPassword);

        return new Credentials(maintenanceUsername, maintenancePassword);
    }

    public Integer getDatabaseVersionNumber(final MysqlDatabaseConnection databaseConnection) {
        return _getDatabaseVersionNumber(databaseConnection);
    }

    public void initializeDatabase(final MysqlDatabaseConnection maintenanceDatabaseConnection) throws DatabaseException {
        final Integer databaseVersionNumber = _getDatabaseVersionNumber(maintenanceDatabaseConnection);

        try {
            if (databaseVersionNumber < 1) {
                final String metadataInitSqlFile = "queries/metadata_init.sql";
                final String query = _getResource(metadataInitSqlFile);
                if (query == null) { throw new RuntimeException("Unable to load: "+ metadataInitSqlFile); }

                _runSqlScript(query, maintenanceDatabaseConnection);

                if (_initSqlFileName != null) {
                    final String initScript = IoUtil.getResource(_initSqlFileName);
                    _runSqlScript(initScript, maintenanceDatabaseConnection);
                }
            }
            else if (databaseVersionNumber < _requiredDatabaseVersion) {
                final Boolean upgradeWasSuccessful = _databaseUpgradeHandler.onUpgrade(databaseVersionNumber, _requiredDatabaseVersion);
                if (! upgradeWasSuccessful) {
                    throw new RuntimeException("Unable to upgrade database from v" + databaseVersionNumber + " to v" + _requiredDatabaseVersion + ".");
                }
            }
        }
        catch (final Exception exception) {
            throw new DatabaseException(exception);
        }
    }
}