/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.jdbc;

import java.sql.SQLException;

import org.postgresql.pljava.internal.Oid;

/**
 * 
 * @author Filip Hrbek
 */

public class ResultSetField {
    private final int    m_len;
    private final int    m_mod;
    private final String m_name;
    private final Oid    m_oid;

    /*
     * Constructor without mod parameter.
     *
     * @param name the name (column name and label) of the field
     * @param oid the OID of the field
     * @param len the length of the field
     */
    public ResultSetField(String name, Oid oid, int len) throws SQLException {
        this(name, oid, len, 0);
    }

    /*
     * Construct a field based on the information fed to it.
     *
     * @param name the name (column name and label) of the field
     * @param oid the OID of the field
     * @param len the length of the field
     */
    public ResultSetField(String name, Oid oid, int len, int mod)
                                                                 throws SQLException {
        m_name = name.toUpperCase();
        m_oid = oid;
        m_len = len;
        m_mod = mod;
    }

    /*
     * @return true if the field can contain a value of specified class
     */
    public final boolean canContain(Class<?> cls) throws SQLException {
        return getJavaClass().isAssignableFrom(cls);
    }

    /*
     * @return the column label of this Field's data type
     */
    public final String getColumnLabel() {
        return m_name;
    }

    /*
     * @return the Java class for oid of this Field's data type
     */
    public final Class<?> getJavaClass() throws SQLException {
        return m_oid.getJavaClass();
    }

    /*
     * @return the length of this Field's data type
     */
    public final int getLength() {
        return m_len;
    }

    /*
     * @return the mod of this Field's data type
     */
    public final int getMod() {
        return m_mod;
    }

    /*
     * @return the oid of this Field's data type
     */
    public final Oid getOID() {
        return m_oid;
    }
}