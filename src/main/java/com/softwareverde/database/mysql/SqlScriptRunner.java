/*
 *  Copyright 2004 Clinton Begin
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 * Modifications:
 *  BenoitDuffez
 *      2015-05 - Slightly modified version of the com.ibatis.common.jdbc.ScriptRunner class
 *                  from the iBATIS Apache project. Only removed dependency on Resource class
 *                  and a constructor
 *      2015-05 - GPSHansl, 06.08.2015: regex for delimiter, rearrange comment/delimiter detection, remove some ide warnings.
 *  Software Verde, LLC
 *      2019-01 - Cloned from: https://github.com/BenoitDuffez/ScriptRunner
 *      2019-01 - Rollbacks aren't attempted if autocommit is set to true.
 *      2019-07 - Renamed to SqlScriptRunner.
 *      2021-01 - Removed sql log creation.
 *      2021-05 - FIX: scripts ending with setting the SQL delimiter no longer attempt to execute an empty command.
 *
 */

package com.softwareverde.database.mysql;

import com.softwareverde.logging.Logger;
import com.softwareverde.util.Util;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Tool to run database scripts
 */
public class SqlScriptRunner {

    private static final String DEFAULT_DELIMITER = ";";
    /**
     * regex to detect delimiter.
     * ignores spaces, allows delimiter in comment, allows an equals-sign
     */
    public static final Pattern delimP = Pattern.compile("^\\s*(--)?\\s*delimiter\\s*=?\\s*([^\\s]+)+\\s*.*$", Pattern.CASE_INSENSITIVE);

    private final Connection connection;

    private final boolean stopOnError;
    private final boolean autoCommit;

    private String delimiter = DEFAULT_DELIMITER;
    private boolean fullLineDelimiter = false;

    /**
     * Default constructor
     */
    public SqlScriptRunner(Connection connection, boolean autoCommit,
                           boolean stopOnError) {
        this.connection = connection;
        this.autoCommit = autoCommit;
        this.stopOnError = stopOnError;
    }

    public void setDelimiter(String delimiter, boolean fullLineDelimiter) {
        this.delimiter = delimiter;
        this.fullLineDelimiter = fullLineDelimiter;
    }

    /**
     * Runs an SQL script (read in using the Reader parameter)
     *
     * @param reader - the source of the script
     */
    public void runScript(Reader reader) throws IOException, SQLException {
        try {
            boolean originalAutoCommit = connection.getAutoCommit();
            try {
                if (originalAutoCommit != this.autoCommit) {
                    connection.setAutoCommit(this.autoCommit);
                }
                runScript(connection, reader);
            } finally {
                connection.setAutoCommit(originalAutoCommit);
            }
        } catch (IOException | SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Error running script.  Cause: " + e, e);
        }
    }

    /**
     * Runs an SQL script (read in using the Reader parameter) using the
     * connection passed in
     *
     * @param conn - the connection to use for the script
     * @param reader - the source of the script
     * @throws SQLException if any SQL errors occur
     * @throws IOException if there is an error reading from the Reader
     */
    private void runScript(Connection conn, Reader reader) throws IOException,
            SQLException {
        StringBuffer command = null;
        try {
            LineNumberReader lineReader = new LineNumberReader(reader);
            String line;
            while ((line = lineReader.readLine()) != null) {
                if (command == null) {
                    command = new StringBuffer();
                }
                String trimmedLine = line.trim();
                final Matcher delimMatch = delimP.matcher(trimmedLine);
                if (trimmedLine.length() < 1
                        || trimmedLine.startsWith("//")) {
                    // Do nothing
                } else if (delimMatch.matches()) {
                    setDelimiter(delimMatch.group(2), false);
                } else if (trimmedLine.startsWith("--")) {
                    // Do nothing
                } else if (!fullLineDelimiter
                        && trimmedLine.endsWith(getDelimiter())
                        || fullLineDelimiter
                        && trimmedLine.equals(getDelimiter())) {
                    command.append(line.substring(0, line
                            .lastIndexOf(getDelimiter())));
                    command.append(" ");
                    this.execCommand(conn, command, lineReader);
                    command = null;
                } else {
                    command.append(line);
                    command.append("\n");
                }
            }
            if (command != null) {
                this.execCommand(conn, command, lineReader);
            }
            if (!autoCommit) {
                conn.commit();
            }
        }
        catch (IOException e) {
            throw new IOException(String.format("Error executing '%s': %s", command, e.getMessage()), e);
        } finally {
            if (! this.autoCommit) {
                conn.rollback();
            }
        }
    }

    private void execCommand(Connection conn, StringBuffer command, LineNumberReader lineReader) throws SQLException {
        final String commandString = (command != null ? command.toString() : null);
        if (Util.isBlank(commandString)) { return; }

        try (Statement statement = conn.createStatement()) {
            try {
                statement.execute(commandString);
            }
            catch (SQLException e) {
                final String errText = String.format("Error executing '%s' (line %d): %s", commandString, lineReader.getLineNumber(), e.getMessage());
                printlnError(errText);

                if (stopOnError) {
                    throw new SQLException(errText, e);
                }
            }

            if (autoCommit && !conn.getAutoCommit()) {
                conn.commit();
            }
        }
    }

    private String getDelimiter() {
        return delimiter;
    }

    private void printlnError(Object o) {
        Logger.error(o);
    }
}
