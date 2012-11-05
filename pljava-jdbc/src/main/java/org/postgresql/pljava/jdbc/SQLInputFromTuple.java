/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Copyright (c) 2010, 2011 PostgreSQL Global Development Group
 *
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://wiki.tada.se/index.php?title=PLJava_License
 */
package org.postgresql.pljava.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLInput;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;

import org.postgresql.pljava.internal.Backend;
import org.postgresql.pljava.internal.JavaWrapper;
import org.postgresql.pljava.internal.TupleDesc;

/**
 * A single row, updateable ResultSet specially made for triggers. The changes
 * made to this ResultSet are remembered and converted to a SPI_modify_tuple
 * call prior to function return.
 * 
 * @author Thomas Hallgren
 */
public class SQLInputFromTuple extends JavaWrapper implements SQLInput {
    private static native Object _getObject(long pointer,
                                            long tupleDescPointer, int index)
                                                                             throws SQLException;

    private int             m_index;
    private final TupleDesc m_tupleDesc;

    private boolean         m_wasNull;

    public SQLInputFromTuple(long heapTupleHeaderPointer, TupleDesc tupleDesc)
                                                                              throws SQLException {
        super(heapTupleHeaderPointer);
        m_tupleDesc = tupleDesc;
        m_index = 0;
        m_wasNull = false;
    }

    public Array readArray() throws SQLException {
        return (Array) readValue(Array.class);
    }

    public InputStream readAsciiStream() throws SQLException {
        Clob c = readClob();
        return c == null ? null : c.getAsciiStream();
    }

    public BigDecimal readBigDecimal() throws SQLException {
        return (BigDecimal) readValue(BigDecimal.class);
    }

    public InputStream readBinaryStream() throws SQLException {
        Blob b = readBlob();
        return b == null ? null : b.getBinaryStream();
    }

    @SuppressWarnings("resource")
    public Blob readBlob() throws SQLException {
        byte[] bytes = readBytes();
        return bytes == null ? null : new BlobValue(bytes);
    }

    public boolean readBoolean() throws SQLException {
        Boolean b = (Boolean) readValue(Boolean.class);
        return b == null ? false : b.booleanValue();
    }

    public byte readByte() throws SQLException {
        Number b = readNumber(byte.class);
        return b == null ? 0 : b.byteValue();
    }

    public byte[] readBytes() throws SQLException {
        return (byte[]) readValue(byte[].class);
    }

    public Reader readCharacterStream() throws SQLException {
        Clob c = readClob();
        return c == null ? null : c.getCharacterStream();
    }

    @SuppressWarnings("resource")
    public Clob readClob() throws SQLException {
        String str = readString();
        return str == null ? null : new ClobValue(str);
    }

    public Date readDate() throws SQLException {
        return (Date) readValue(Date.class);
    }

    public double readDouble() throws SQLException {
        Number d = readNumber(double.class);
        return d == null ? 0 : d.doubleValue();
    }

    public float readFloat() throws SQLException {
        Number f = readNumber(float.class);
        return f == null ? 0 : f.floatValue();
    }

    public int readInt() throws SQLException {
        Number i = readNumber(int.class);
        return i == null ? 0 : i.intValue();
    }

    public long readLong() throws SQLException {
        Number l = readNumber(long.class);
        return l == null ? 0 : l.longValue();
    }

    public NClob readNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                                                  this.getClass()
                                                          + ".readNClob() not implemented yet.",
                                                  "0A000");

    }

    public String readNString() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                                                  this.getClass()
                                                          + ".readNString() not implemented yet.",
                                                  "0A000");

    }

    public Object readObject() throws SQLException {
        if (m_index < m_tupleDesc.size()) {
            Object v;
            synchronized (Backend.THREADLOCK) {
                v = _getObject(getNativePointer(),
                               m_tupleDesc.getNativePointer(), ++m_index);
            }
            m_wasNull = v == null;
            return v;
        }
        throw new SQLException("Tuple has no more columns");
    }

    public Ref readRef() throws SQLException {
        return (Ref) readValue(Ref.class);
    }

    public RowId readRowId() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                                                  this.getClass()
                                                          + ".readRowId() not implemented yet.",
                                                  "0A000");
    }

    public short readShort() throws SQLException {
        Number s = readNumber(short.class);
        return s == null ? 0 : s.shortValue();
    }

    public SQLXML readSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                                                  this.getClass()
                                                          + ".readSQLXML() not implemented yet.",
                                                  "0A000");
    }

    public String readString() throws SQLException {
        return (String) readValue(String.class);
    }

    public Time readTime() throws SQLException {
        return (Time) readValue(Time.class);
    }

    public Timestamp readTimestamp() throws SQLException {
        return (Timestamp) readValue(Timestamp.class);
    }

    public URL readURL() throws SQLException {
        return (URL) readValue(URL.class);
    }

    public boolean wasNull() throws SQLException {
        return m_wasNull;
    }

    private Number readNumber(Class<?> numberClass) throws SQLException {
        return SPIConnection.basicNumericCoersion(numberClass, readObject());
    }

    // ************************************************************
    // End of non-implementation of JDBC 4 methods.
    // ************************************************************

    private Object readValue(Class<?> valueClass) throws SQLException {
        return SPIConnection.basicCoersion(valueClass, readObject());
    }

    // ************************************************************
    // Non-implementation of JDBC 4 methods.
    // ************************************************************

    @Override
    protected native void _free(long pointer);
}
