/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * A Synthetic ResultSet that provides direct access to data stored in a
 * {@link java.util.ArrayList}. This kind of ResultSet has nothing common with
 * any statement.
 * 
 * @author Filip Hrbek
 */
public class SyntheticResultSet extends ResultSetBase {
    private final HashMap<String, Integer> m_fieldIndexes;
    private final ResultSetField[]         m_fields;
    private final ArrayList<?>             m_tuples;

    SyntheticResultSet(ResultSetField[] fields, ArrayList<?> tuples)
                                                                    throws SQLException {
        super(tuples.size());
        m_fields = fields;
        m_tuples = tuples;
        m_fieldIndexes = new HashMap<String, Integer>();
        int i = m_fields.length;
        while (--i >= 0) {
            m_fieldIndexes.put(m_fields[i].getColumnLabel(), new Integer(i + 1));
        }

        Object[][] tupleTest = m_tuples.toArray(new Object[0][]);
        Object value;
        for (i = 0; i < tupleTest.length; i++) {
            int j = m_fields.length;
            while (--j >= 0) {
                value = tupleTest[i][j];
                if (value != null && !m_fields[j].canContain(value.getClass())) {
                    throw new SQLException("Unable to store class "
                                           + value.getClass()
                                           + " in ResultSetField '"
                                           + m_fields[j].getColumnLabel() + "'"
                                           + " with OID "
                                           + m_fields[j].getOID()
                                           + " (expected class: "
                                           + m_fields[j].getJavaClass() + ")");
                }
            }
        }
    }

    @Override
    public void close() throws SQLException {
        m_tuples.clear();
        super.close();
    }

    public int findColumn(String columnName) throws SQLException {
        Integer idx = m_fieldIndexes.get(columnName.toUpperCase());
        if (idx != null) {
            return idx.intValue();
        }
        throw new SQLException("No such field: '" + columnName + "'");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new SyntheticResultSetMetaData(m_fields);
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

    public boolean isLast() throws SQLException {
        return getRow() == m_tuples.size();
    }

    public boolean next() throws SQLException {
        int row = getRow();
        if (row < m_tuples.size()) {
            setRow(row + 1);
            return true;
        }
        return false;
    }

    protected final Object[] getCurrentRow() throws SQLException {
        int row = getRow();
        if (row < 1 || row > m_tuples.size()) {
            throw new SQLException("ResultSet is not positioned on a valid row");
        }
        return (Object[]) m_tuples.get(row - 1);
    }

    @Override
    protected Object getObjectValue(int columnIndex) throws SQLException {
        return getCurrentRow()[columnIndex - 1];
    }
}
