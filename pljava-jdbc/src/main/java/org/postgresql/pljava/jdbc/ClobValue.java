/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Copyright (c) 2010, 2011 PostgreSQL Global Development Group
 *
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://wiki.tada.se/index.php?title=PLJava_License
 */
package org.postgresql.pljava.jdbc;

import java.io.BufferedInputStream;
import java.io.CharConversionException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

/**
 * @author Thomas Hallgren
 */
public class ClobValue extends Reader implements Clob {
    public static int getReaderLength(Reader value) throws SQLException {
        try {
            value.mark(Integer.MAX_VALUE);
            long length = value.skip(Long.MAX_VALUE);
            if (length > Integer.MAX_VALUE) {
                throw new SQLException("stream content too large");
            }
            value.reset();
            return (int) length;
        } catch (IOException e) {
            throw new SQLException(e.getMessage());
        }
    }

    private long         m_markPos;

    private final long   m_nChars;

    private final Reader m_reader;

    private long         m_readerPos;

    public ClobValue(Reader reader, long nChars) {
        m_reader = reader;
        m_nChars = nChars;
        m_readerPos = 0L;
        m_markPos = 0L;
    }

    public ClobValue(String value) {
        this(new StringReader(value), value.length());
    }

    @Override
    public void close() throws IOException {
        m_reader.close();
        m_readerPos = 0;
        m_markPos = 0;
    }

    public void free() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                                                  this.getClass()
                                                          + ".free() not implemented yet.",
                                                  "0A000");
    }

    public InputStream getAsciiStream() {
        return new BufferedInputStream(new InputStream() {
            @Override
            public int read() throws IOException {
                int nextChar = ClobValue.this.read();
                if (nextChar > 127) {
                    throw new CharConversionException(
                                                      "Non ascii character in Clob data");
                }
                return nextChar;
            }
        });
    }

    public Reader getCharacterStream() {
        return this;
    }

    public Reader getCharacterStream(long pos, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                                                  this.getClass()
                                                          + ".getCharacterStream( long,long ) not implemented yet.",
                                                  "0A000");
    }

    public String getSubString(long pos, int length) throws SQLException {
        if (pos < 0L || length < 0) {
            throw new IllegalArgumentException();
        }
        if (length == 0) {
            return "";
        }

        if (pos + length > m_nChars) {
            throw new SQLException("Attempt to read beyond end of Clob data");
        }

        long skip = pos - m_readerPos;
        if (skip < 0) {
            throw new SQLException("Cannot position Clob stream backwards");
        }

        try {
            if (skip > 0) {
                skip(skip);
            }

            char[] buf = new char[length];
            int nr = this.read(buf);
            if (nr < length) {
                throw new SQLException("Clob data read not fulfilled");
            }
            return new String(buf);
        } catch (IOException e) {
            throw new SQLException("Error reading Blob data: " + e.getMessage());
        }
    }

    public long length() {
        return m_nChars;
    }

    @Override
    public synchronized void mark(int readLimit) throws IOException {
        m_reader.mark(readLimit);
        m_markPos = m_readerPos;
    }

    @Override
    public boolean markSupported() {
        return m_reader.markSupported();
    }

    /**
     * Not supported.
     */
    public long position(Clob pattern, long start) {
        throw new UnsupportedOperationException();
    }

    /**
     * In this method is not supported by <code>ClobValue</code>
     */
    public long position(String pattern, long start) {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized int read() throws IOException {
        int rs = m_reader.read();
        m_readerPos++;
        return rs;
    }

    @Override
    public synchronized int read(char[] b) throws IOException {
        int rs = m_reader.read(b);
        m_readerPos += rs;
        return rs;
    }

    @Override
    public synchronized int read(char[] b, int off, int len) throws IOException {
        int rs = m_reader.read(b, off, len);
        m_readerPos += rs;
        return rs;
    }

    @Override
    public synchronized boolean ready() throws IOException {
        return m_reader.ready();
    }

    @Override
    public synchronized void reset() throws IOException {
        m_reader.reset();
        m_readerPos = m_markPos;
    }

    /**
     * In this method is not supported by <code>ClobValue</code>
     */
    public OutputStream setAsciiStream(long pos) {
        throw new UnsupportedOperationException();
    }

    /**
     * In this method is not supported by <code>ClobValue</code>
     */
    public Writer setCharacterStream(long pos) {
        throw new UnsupportedOperationException();
    }

    /**
     * In this method is not supported by <code>ClobValue</code>
     */
    public int setString(long pos, String str) {
        throw new UnsupportedOperationException();
    }

    /**
     * In this method is not supported by <code>ClobValue</code>
     */
    public int setString(long pos, String str, int offset, int len) {
        throw new UnsupportedOperationException();
    }

    //************************************************************
    // Non-implementation of JDBC 4 methods.
    //************************************************************

    @Override
    public synchronized long skip(long nBytes) throws IOException {
        long skipped = m_reader.skip(nBytes);
        m_readerPos += skipped;
        return skipped;
    }

    /**
     * In this method is not supported by <code>ClobValue</code>
     */
    public void truncate(long len) {
        throw new UnsupportedOperationException();
    }
}
