package com.softwareverde.database.mysql;

import com.softwareverde.database.DatabaseConnection;
import com.softwareverde.database.DatabaseException;
import com.softwareverde.database.properties.DatabaseCredentials;
import com.softwareverde.database.properties.DatabaseProperties;
import com.softwareverde.database.query.Query;
import com.softwareverde.database.row.Row;
import com.softwareverde.database.util.TransactionUtil;
import com.softwareverde.util.HexUtil;
import com.softwareverde.util.IoUtil;
import com.softwareverde.util.StringUtil;

import java.io.InputStream;
import java.io.StringReader;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.util.List;

public class MysqlDatabaseInitializer implements com.softwareverde.database.DatabaseInitializer<Connection> {

    protected final String _initSqlFileName;
    protected final Integer _requiredDatabaseVersion;
    protected final DatabaseUpgradeHandler<Connection> _databaseUpgradeHandler;

    protected String _hashPassword(final String password) {
        try {
            try {
                final MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
                final byte[] bytes = messageDigest.digest(StringUtil.stringToBytes(password));
                return HexUtil.toHexString(bytes);
            }
            catch (final NoSuchAlgorithmException exception) {
                throw new RuntimeException(exception);
            }
        }
        catch (final Exception exception) {
            throw new RuntimeException(exception);
        }
    }

    protected String _getResource(final String resourceFile) {
        final ClassLoader classLoader = this.getClass().getClassLoader();
        final InputStream resourceStream = classLoader.getResourceAsStream(resourceFile);
        if (resourceStream == null) { return null; }
        return IoUtil.streamToString(resourceStream);
    }

    protected Integer _getDatabaseVersionNumber(final DatabaseConnection<Connection> databaseConnection) {
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

    protected void _runSqlScript(final String databaseInitFileContents, final DatabaseConnection<Connection> databaseConnection) throws DatabaseException {
        try {
            TransactionUtil.startTransaction(databaseConnection);
            final SqlScriptRunner scriptRunner = new SqlScriptRunner(databaseConnection.getRawConnection(), false, true);
            scriptRunner.runScript(new StringReader(databaseInitFileContents));
            TransactionUtil.commitTransaction(databaseConnection);
        }
        catch (final Exception exception) {
            throw new DatabaseException(exception);
        }
    }

    public MysqlDatabaseInitializer() {
        _initSqlFileName = null;
        _requiredDatabaseVersion = 1;
        _databaseUpgradeHandler = new DatabaseUpgradeHandler<Connection>() {
            @Override
            public Boolean onUpgrade(final DatabaseConnection<Connection> maintenanceDatabaseConnection, final Integer previousVersion, final Integer requiredVersion) {
                throw new RuntimeException("Database upgrade not supported.");
            }
        };
    }

    public MysqlDatabaseInitializer(final String databaseInitFileName, final Integer requiredDatabaseVersion, final DatabaseUpgradeHandler<Connection> databaseUpgradeHandler) {
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
     */
    @Override
    public void initializeSchema(final DatabaseConnection<Connection> rootDatabaseConnection, final DatabaseProperties databaseProperties) throws DatabaseException {
        final DatabaseCredentials credentials;
        final DatabaseCredentials maintenanceCredentials;
        {
            final String databaseSchema = databaseProperties.getSchema();
            final String newRootPassword = databaseProperties.getRootPassword();
            final String maintenanceUsername = (databaseSchema + "_maintenance");
            final String maintenancePassword = _hashPassword(newRootPassword);

            credentials = new DatabaseCredentials(databaseProperties.getUsername(), databaseProperties.getPassword());
            maintenanceCredentials = new DatabaseCredentials(maintenanceUsername, maintenancePassword);
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

    @Override
    public DatabaseCredentials getMaintenanceCredentials(final DatabaseProperties databaseProperties) {
        final String databaseSchema = databaseProperties.getSchema();
        final String rootPassword = databaseProperties.getRootPassword();
        final String maintenanceUsername = (databaseSchema + "_maintenance");
        final String maintenancePassword = _hashPassword(rootPassword);

        return new DatabaseCredentials(maintenanceUsername, maintenancePassword);
    }

    @Override
    public Integer getDatabaseVersionNumber(final DatabaseConnection<Connection> databaseConnection) {
        return _getDatabaseVersionNumber(databaseConnection);
    }

    @Override
    public void initializeDatabase(final DatabaseConnection<Connection> maintenanceDatabaseConnection) throws DatabaseException {
        try {
            { // Check/Handle Database Initialization....
                final Integer databaseVersionNumber = _getDatabaseVersionNumber(maintenanceDatabaseConnection);
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
            }

            { // Check/Handle Database Upgrade...
                final Integer databaseVersionNumber = _getDatabaseVersionNumber(maintenanceDatabaseConnection); // Get the updated database version after initialization...
                if (databaseVersionNumber < _requiredDatabaseVersion) {
                    final Boolean upgradeWasSuccessful = _databaseUpgradeHandler.onUpgrade(maintenanceDatabaseConnection, databaseVersionNumber, _requiredDatabaseVersion);
                    if (! upgradeWasSuccessful) {
                        throw new RuntimeException("Unable to upgrade database from v" + databaseVersionNumber + " to v" + _requiredDatabaseVersion + ".");
                    }
                }
            }
        }
        catch (final Exception exception) {
            throw new DatabaseException(exception);
        }
    }
}