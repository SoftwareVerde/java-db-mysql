package com.softwareverde.database.mysql.properties;

import com.softwareverde.constable.Const;

public class Credentials implements Const {
    public final String username;
    public final String password;

    public Credentials(final String username, final String password) {
        this.username = username;
        this.password = password;
    }
}
