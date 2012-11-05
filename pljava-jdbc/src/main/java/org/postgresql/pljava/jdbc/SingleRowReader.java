/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Copyright (c) 2010, 2011 PostgreSQL Global Development Group
 *
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://wiki.tada.se/index.php?title=PLJava_License
 */
package org.postgresql.pljava.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.postgresql.pljava.internal.Backend;
import org.postgresql.pljava.internal.TupleDesc;

/**
 * A single row, read-only ResultSet, specially made for functions and
 * procedures that takes complex types as arguments (PostgreSQL 7.5 and later).
 * 
 * @author Thomas Hallgren
 */
public class SingleRowReader extends SingleRowResultSet {
    private static native Object _getObject(long pointer,
                                            long tupleDescPointer, int index)
                                                                             throws SQLException;

    private static SQLException readOnlyException() {
        return new UnsupportedFeatureException("ResultSet is read-only");
    }

    private final long      m_pointer;

    private final TupleDesc m_tupleDesc;

    public SingleRowReader(long pointer, TupleDesc tupleDesc)
                                                             throws SQLException {
        m_pointer = pointer;
        m_tupleDesc = tupleDesc;
    }

    /**
     * This feature is not supported on a <code>ReadOnlyResultSet</code>.
     * 
     * @throws SQLException
     *             indicating that this feature is not supported.
     */
    public void cancelRowUpdates() throws SQLException {
        throw readOnlyException();
    }

    public void close() {
    }

    /**
     * This feature is not supported on a <code>ReadOnlyResultSet</code>.
     * 
     * @throws SQLException
     *             indicating that this feature is not supported.
     */
    @Override
    public void deleteRow() throws SQLException {
        throw readOnlyException();
    }

    @Override
    public void finalize() {
        synchronized (Backend.THREADLOCK) {
            _free(m_pointer);
        }
    }

    /**
     * Returns {@link ResultSet#CONCUR_READ_ONLY}.
     */
    @Override
    public int getConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    /* (non-Javadoc)
     * @see java.sql.ResultSet#getObject(int, java.lang.Class)
     */
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see java.sql.ResultSet#getObject(java.lang.String, java.lang.Class)
     */
    public <T> T getObject(String columnLabel, Class<T> type)
                                                             throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * This feature is not supported on a <code>ReadOnlyResultSet</code>.
     * 
     * @throws SQLException
     *             indicating that this feature is not supported.
     */
    @Override
    public void insertRow() throws SQLException {
        throw readOnlyException();
    }

    public boolean isClosed() throws SQLException {
        return false;
    }

    /**
     * This feature is not supported on a <code>ReadOnlyResultSet</code>.
     * 
     * @throws SQLException
     *             indicating that this feature is not supported.
     */
    @Override
    public void moveToInsertRow() throws SQLException {
        throw readOnlyException();
    }

    /**
     * Always returns false.
     */
    public boolean rowUpdated() throws SQLException {
        return false;
    }

    // ************************************************************
    // Implementation of JDBC 4 methods.
    // ************************************************************

    /**
     * This feature is not supported on a <code>ReadOnlyResultSet</code>.
     * 
     * @throws SQLException
     *             indicating that this feature is not supported.
     */
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw readOnlyException();
    }

    // ************************************************************
    // End of implementation of JDBC 4 methods.
    // ************************************************************

    /**
     * This feature is not supported on a <code>ReadOnlyResultSet</code>.
     * 
     * @throws SQLException
     *             indicating that this feature is not supported.
     */
    @Override
    public void updateObject(int columnIndex, Object x, int scale)
                                                                  throws SQLException {
        throw readOnlyException();
    }

    /**
     * This feature is not supported on a <code>ReadOnlyResultSet</code>.
     * 
     * @throws SQLException
     *             indicating that this feature is not supported.
     */
    @Override
    public void updateRow() throws SQLException {
        throw readOnlyException();
    }

    protected native void _free(long pointer);

    @Override
    protected Object getObjectValue(int columnIndex) throws SQLException {
        synchronized (Backend.THREADLOCK) {
            return _getObject(m_pointer, m_tupleDesc.getNativePointer(),
                              columnIndex);
        }
    }

    @Override
    protected final TupleDesc getTupleDesc() {
        return m_tupleDesc;
    }
}
