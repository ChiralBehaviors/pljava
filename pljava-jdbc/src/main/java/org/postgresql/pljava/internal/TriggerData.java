/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.internal;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.postgresql.pljava.jdbc.TriggerResultSet;

/**
 * The <code>TriggerData</code> correspons to the internal PostgreSQL
 * <code>TriggerData</code>.
 * 
 * @author Thomas Hallgren
 */
public class TriggerData extends JavaWrapper implements
        org.postgresql.pljava.TriggerData {
    private static native String[] _getArguments(long pointer)
                                                              throws SQLException;

    private static native String _getName(long pointer) throws SQLException;

    private static native Tuple _getNewTuple(long pointer) throws SQLException;

    private static native Relation _getRelation(long pointer)
                                                             throws SQLException;

    private static native Tuple _getTriggerTuple(long pointer)
                                                              throws SQLException;

    private static native boolean _isFiredAfter(long pointer)
                                                             throws SQLException;

    private static native boolean _isFiredBefore(long pointer)
                                                              throws SQLException;

    private static native boolean _isFiredByDelete(long pointer)
                                                                throws SQLException;

    private static native boolean _isFiredByInsert(long pointer)
                                                                throws SQLException;

    private static native boolean _isFiredByUpdate(long pointer)
                                                                throws SQLException;

    private static native boolean _isFiredForEachRow(long pointer)
                                                                  throws SQLException;

    private static native boolean _isFiredForStatement(long pointer)
                                                                    throws SQLException;

    private TriggerResultSet m_new = null;

    private Tuple            m_newTuple;

    private TriggerResultSet m_old = null;

    private Relation         m_relation;

    private Tuple            m_triggerTuple;

    TriggerData(long pointer) {
        super(pointer);
    }

    /**
     * Returns the arguments for this trigger (as declared in the
     * <code>CREATE TRIGGER</code> statement. If the trigger has no arguments,
     * this method will return an array with size 0.
     * 
     * @throws SQLException
     *             if the contained native buffer has gone stale.
     */
    public String[] getArguments() throws SQLException {
        synchronized (Backend.THREADLOCK) {
            return _getArguments(getNativePointer());
        }
    }

    /**
     * Returns the name of the trigger (as declared in the
     * <code>CREATE TRIGGER</code> statement).
     * 
     * @throws SQLException
     *             if the contained native buffer has gone stale.
     */
    public String getName() throws SQLException {
        synchronized (Backend.THREADLOCK) {
            return _getName(getNativePointer());
        }
    }

    /**
     * Returns the ResultSet that represents the new row. This ResultSet will be
     * null for delete triggers and for triggers that was fired for statement. <br/>
     * The returned set will be updateable and positioned on a valid row.
     * 
     * @return An updateable <code>ResultSet</code> containing one row or
     *         <code>null</code>.
     * @throws SQLException
     *             if the contained native buffer has gone stale.
     */
    public ResultSet getNew() throws SQLException {
        if (m_new != null) {
            return m_new;
        }

        if (isFiredByDelete() || isFiredForStatement()) {
            return null;
        }

        // PostgreSQL uses the trigger tuple as the new tuple for inserts.
        //
        Tuple tuple = isFiredByInsert() ? getTriggerTuple() : getNewTuple();

        // Triggers fired after will always have a read-only row
        //
        m_new = new TriggerResultSet(getRelation().getTupleDesc(), tuple,
                                     isFiredAfter());
        return m_new;
    }

    /**
     * Returns a <code>Tuple</code> reflecting the new version of the row, if
     * the trigger was fired for an <code>UPDATE</code>, and <code>null</code>
     * if it is for an <code>INSERT</code> or a <code>DELETE</code>. This is
     * what you have to return from the function if the event is an
     * <code>UPDATE</code> and you don't want to replace this row by a different
     * one or skip the operation.
     * 
     * @throws SQLException
     *             if the contained native buffer has gone stale.
     */
    public Tuple getNewTuple() throws SQLException {
        if (m_newTuple == null) {
            synchronized (Backend.THREADLOCK) {
                m_newTuple = _getNewTuple(getNativePointer());
            }
        }
        return m_newTuple;
    }

    /**
     * Returns the ResultSet that represents the old row. This ResultSet will be
     * null for insert triggers and for triggers that was fired for statement. <br/>
     * The returned set will be read-only and positioned on a valid row.
     * 
     * @return A read-only <code>ResultSet</code> containing one row or
     *         <code>null</code>.
     * @throws SQLException
     *             if the contained native buffer has gone stale.
     */
    public ResultSet getOld() throws SQLException {
        if (m_old != null) {
            return m_old;
        }

        if (isFiredByInsert() || isFiredForStatement()) {
            return null;
        }
        m_old = new TriggerResultSet(getRelation().getTupleDesc(),
                                     getTriggerTuple(), true);
        return m_old;
    }

    /**
     * Returns a descriptor for the Tuples exposed by this trigger.
     * 
     * @throws SQLException
     *             if the contained native buffer has gone stale.
     */
    public Relation getRelation() throws SQLException {
        if (m_relation == null) {
            synchronized (Backend.THREADLOCK) {
                m_relation = _getRelation(getNativePointer());
            }
        }
        return m_relation;
    }

    public String getSchemaName() throws SQLException {
        return getRelation().getSchema();
    }

    public String getTableName() throws SQLException {
        return getRelation().getName();
    }

    /**
     * Commits the changes made on the <code>ResultSet</code> representing
     * <code>new</code> and returns the native pointer of new tuple. This method
     * is called automatically by the trigger handler and should not be called
     * in any other way.
     * 
     * @return The modified tuple, or if no modifications have been made, the
     *         original tuple.
     */
    public long getTriggerReturnTuple() throws SQLException {
        if (isFiredForStatement() || isFiredAfter()) {
            //
            // Only triggers fired before each row can have a return
            // value.
            //
            return 0;
        }

        if (m_new != null) {
            Object[] changes = m_new.getChangeIndexesAndValues();
            if (changes != null) {
                Tuple original = (Tuple) changes[0];
                int[] indexes = (int[]) changes[1];
                Object[] values = (Object[]) changes[2];
                return getRelation().modifyTuple(original, indexes, values).getNativePointer();
            }
        }

        // Return the original tuple.
        //
        return (isFiredByUpdate() ? getNewTuple() : getTriggerTuple()).getNativePointer();
    }

    /**
     * Returns a <code>Tuple</code> reflecting the row for which the trigger was
     * fired. This is the row being inserted, updated, or deleted. If this
     * trigger was fired for an <code>
     * INSERT</code> or <code>DELETE</code> then this is what you should return
     * to from the method if you don't want to replace the row with a different
     * one (in the case of <code>INSERT
     * </code>) or skip the operation.
     * 
     * @throws SQLException
     *             if the contained native buffer has gone stale.
     */
    public Tuple getTriggerTuple() throws SQLException {
        if (m_triggerTuple == null) {
            synchronized (Backend.THREADLOCK) {
                m_triggerTuple = _getTriggerTuple(getNativePointer());
            }
        }
        return m_triggerTuple;
    }

    /**
     * Returns <code>true</code> if the trigger was fired after the statement or
     * row action that it is associated with.
     * 
     * @throws SQLException
     *             if the contained native buffer has gone stale.
     */
    public boolean isFiredAfter() throws SQLException {
        synchronized (Backend.THREADLOCK) {
            return _isFiredAfter(getNativePointer());
        }
    }

    /**
     * Returns <code>true</code> if the trigger was fired before the statement
     * or row action that it is associated with.
     * 
     * @throws SQLException
     *             if the contained native buffer has gone stale.
     */
    public boolean isFiredBefore() throws SQLException {
        synchronized (Backend.THREADLOCK) {
            return _isFiredBefore(getNativePointer());
        }
    }

    /**
     * Returns <code>true</code> if this trigger was fired by a
     * <code>DELETE</code>.
     * 
     * @throws SQLException
     *             if the contained native buffer has gone stale.
     */
    public boolean isFiredByDelete() throws SQLException {
        synchronized (Backend.THREADLOCK) {
            return _isFiredByDelete(getNativePointer());
        }
    }

    /**
     * Returns <code>true</code> if this trigger was fired by an
     * <code>INSERT</code>.
     * 
     * @throws SQLException
     *             if the contained native buffer has gone stale.
     */
    public boolean isFiredByInsert() throws SQLException {
        synchronized (Backend.THREADLOCK) {
            return _isFiredByInsert(getNativePointer());
        }
    }

    /**
     * Returns <code>true</code> if this trigger was fired by an
     * <code>UPDATE</code>.
     * 
     * @throws SQLException
     *             if the contained native buffer has gone stale.
     */
    public boolean isFiredByUpdate() throws SQLException {
        synchronized (Backend.THREADLOCK) {
            return _isFiredByUpdate(getNativePointer());
        }
    }

    /**
     * Returns <code>true</code> if this trigger is fired once for each row (as
     * opposed to once for the entire statement).
     * 
     * @throws SQLException
     *             if the contained native buffer has gone stale.
     */
    public boolean isFiredForEachRow() throws SQLException {
        synchronized (Backend.THREADLOCK) {
            return _isFiredForEachRow(getNativePointer());
        }
    }

    /**
     * Returns <code>true</code> if this trigger is fired once for the entire
     * statement (as opposed to once for each row).
     * 
     * @throws SQLException
     *             if the contained native buffer has gone stale.
     */
    public boolean isFiredForStatement() throws SQLException {
        synchronized (Backend.THREADLOCK) {
            return _isFiredForStatement(getNativePointer());
        }
    }

    @Override
    protected native void _free(long pointer);
}
