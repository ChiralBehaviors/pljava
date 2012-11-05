/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.internal;

import java.sql.SQLException;

/**
 * The <code>AclId</code> correspons to the internal PostgreSQL
 * <code>AclId</code>.
 * 
 * @author Thomas Hallgren
 */
public final class AclId {
    /**
     * Return the id of the session user.
     * 
     * @throws SQLException
     *             if the user is unknown to the system.
     */
    public static AclId fromName(String name) throws SQLException {
        synchronized (Backend.THREADLOCK) {
            return _fromName(name);
        }
    }

    /**
     * Return the id of the session user.
     */
    public static AclId getSessionUser() {
        synchronized (Backend.THREADLOCK) {
            return _getSessionUser();
        }
    }

    /**
     * Return the id of the current database user.
     */
    public static AclId getUser() {
        synchronized (Backend.THREADLOCK) {
            return _getUser();
        }
    }

    private static native AclId _fromName(String name);

    private static native AclId _getSessionUser();

    private static native AclId _getUser();

    private final int m_native;

    /**
     * Called from native code.
     */
    public AclId(int nativeAclId) {
        m_native = nativeAclId;
    }

    /**
     * Returns equal if other is an AclId that is equal to this id.
     */
    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof AclId
               && ((AclId) other).m_native == m_native;
    }

    /**
     * Return the name that corresponds to this id.
     */
    public String getName() {
        synchronized (Backend.THREADLOCK) {
            return _getName();
        }
    }

    /**
     * Returns the hashCode of this id.
     */
    @Override
    public int hashCode() {
        return m_native;
    }

    /**
     * Return true if this AclId has the right to create new objects in the
     * given schema.
     */
    public boolean hasSchemaCreatePermission(Oid oid) {
        synchronized (Backend.THREADLOCK) {
            return _hasSchemaCreatePermission(oid);
        }
    }

    /**
     * Returns the integer value of this id.
     */
    public int intValue() {
        return m_native;
    }

    /**
     * Returns true if this AclId represents a super user.
     */
    public boolean isSuperuser() {
        synchronized (Backend.THREADLOCK) {
            return _isSuperuser();
        }
    }

    /**
     * Returns the result of calling #getName().
     */
    @Override
    public String toString() {
        return getName();
    }

    private native String _getName();

    private native boolean _hasSchemaCreatePermission(Oid oid);

    private native boolean _isSuperuser();
}
