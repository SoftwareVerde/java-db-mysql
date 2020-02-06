package com.softwareverde.database.mysql;

import org.junit.Assert;
import org.junit.Test;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class DatabaseInitializerTests {
    public static String originalSha256(final String s) {
        try {
            final MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
            final byte[] array = messageDigest.digest(s.getBytes());
            final StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0; i < array.length; ++i) {
                stringBuilder.append(Integer.toHexString((array[i] & 0xFF) | 0x0100).substring(1,3));
            }
            return stringBuilder.toString();
        }
        catch (final NoSuchAlgorithmException exception) {
            return null;
        }
    }

    @Test
    public void should_replicate_old_password_format() {
        // Setup
        final MysqlDatabaseInitializer mysqlDatabaseInitializer = new MysqlDatabaseInitializer();
        final String password = "0123456789ABCDEFFEDCBA9876543210";

        final String originalPassword = DatabaseInitializerTests.originalSha256(password);

        // Action
        final String newPassword = mysqlDatabaseInitializer._hashPassword(password);

        // Assert
        Assert.assertEquals(originalPassword, newPassword);
    }
}
