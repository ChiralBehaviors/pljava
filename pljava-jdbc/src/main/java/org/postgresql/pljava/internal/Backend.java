/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.internal;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FilePermission;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.security.Permission;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.PropertyPermission;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.postgresql.pljava.jdbc.SQLUtils;

/**
 * Provides access to some useful routines in the PostgreSQL server.
 * 
 * @author Thomas Hallgren
 */
public class Backend {
    private static class PLJavaSecurityManager extends SecurityManager {
        private boolean m_recursion = false;

        @Override
        public void checkPermission(Permission perm) {
            nonRecursiveCheck(perm);
        }

        @Override
        public void checkPermission(Permission perm, Object context) {
            nonRecursiveCheck(perm);
        }

        private synchronized void nonRecursiveCheck(Permission perm) {
            if (m_recursion) {
                //
                // Something, probably a ClassLoader
                // loading one of the referenced
                // classes, caused a recursion. Well
                // everything done within this method
                // is permitted so we just return
                // here.
                //
                return;
            }

            m_recursion = true;
            try {
                assertPermission(perm);
            } finally {
                m_recursion = false;
            }
        }

        void assertPermission(Permission perm) {
            if (perm instanceof RuntimePermission) {
                String name = perm.getName();
                if ("*".equals(name) || "exitVM".equals(name)) {
                    throw new SecurityException();
                } else if ("setSecurityManager".equals(name) && !s_inSetTrusted) {
                    //
                    // Attempt to set another
                    // security manager while not
                    // in the setTrusted method
                    //
                    throw new SecurityException();
                }
            } else if (perm instanceof PropertyPermission) {
                if (perm.getActions().indexOf("write") >= 0) {
                    // We never allow this to be changed.
                    //
                    String propName = perm.getName();
                    if (propName.equals("java.home")) {
                        throw new SecurityException();
                    }
                }
            }
        }
    }

    /**
     * All native calls synchronize on this object.
     */
    public static final Object           THREADLOCK                 = new Object();

    private static boolean               s_inSetTrusted             = false;

    private static Session               s_session;

    /**
     * This security manager will block all attempts to access the file system
     */
    private static final SecurityManager s_trustedSecurityManager   = new PLJavaSecurityManager() {
                                                                        @Override
                                                                        void assertPermission(Permission perm) {
                                                                            if (perm instanceof FilePermission) {
                                                                                String actions = perm.getActions();
                                                                                if ("read".equals(actions)) {
                                                                                    // Allow read of /dev/random
                                                                                    // and /dev/urandom

                                                                                    String fileName = perm.getName();

                                                                                    if ("/dev/random".equals(fileName)
                                                                                        || "/dev/urandom".equals(fileName)) {
                                                                                        return;
                                                                                    }

                                                                                    // Must be able to read
                                                                                    // timezone info etc. in the
                                                                                    // java installation
                                                                                    // directory.
                                                                                    //
                                                                                    File javaHome = new File(
                                                                                                             System.getProperty("java.home"));
                                                                                    File accessedFile = new File(
                                                                                                                 perm.getName());
                                                                                    File fileDir = accessedFile.getParentFile();
                                                                                    while (fileDir != null) {
                                                                                        if (fileDir.equals(javaHome)) {
                                                                                            return;
                                                                                        }
                                                                                        fileDir = fileDir.getParentFile();
                                                                                    }
                                                                                }
                                                                                throw new SecurityException(
                                                                                                            perm.getActions()
                                                                                                                    + " on "
                                                                                                                    + perm.getName());
                                                                            }
                                                                            super.assertPermission(perm);
                                                                        }
                                                                    };

    private static final SecurityManager s_untrustedSecurityManager = new PLJavaSecurityManager();

    /**
     * Reads the jar found at the specified URL and stores the entries in the
     * jar_entry table.
     * 
     * @param jarId
     *            The id used for the foreign key to the jar_repository table
     * @param urlStream
     *            The URL
     * @throws SQLException
     */
    public static void addClassImages(int jarId, InputStream urlStream)
                                                                       throws SQLException {
        PreparedStatement stmt = null;
        PreparedStatement descIdStmt = null;
        ResultSet rs = null;
        JarInputStream jis = null;
    
        try {
            int deployImageId = -1;
            byte[] buf = new byte[1024];
            ByteArrayOutputStream img = new ByteArrayOutputStream();
            stmt = SQLUtils.getDefaultConnection().prepareStatement("INSERT INTO sqlj.jar_entry(entryName, jarId, entryImage) VALUES(?, ?, ?)");
    
            jis = new JarInputStream(urlStream);
            Manifest manifest = jis.getManifest();
            if (manifest != null) {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                manifest.write(out);
                PreparedStatement us = SQLUtils.getDefaultConnection().prepareStatement("UPDATE sqlj.jar_repository SET jarManifest = ? WHERE jarId = ?");
                try {
                    us.setString(1, new String(out.toByteArray(), "UTF8"));
                    us.setInt(2, jarId);
                    if (us.executeUpdate() != 1) {
                        throw new SQLException(
                                               "Jar repository update did not update 1 row");
                    }
                } catch (UnsupportedEncodingException e) {
                    // Excuse me? No UTF8 encoding?
                    //
                    throw new SQLException("JVM does not support UTF8!!");
                } finally {
                    SQLUtils.close(us);
                }
            }
            for (;;) {
                JarEntry je = jis.getNextJarEntry();
                if (je == null) {
                    break;
                }
    
                if (je.isDirectory()) {
                    continue;
                }
    
                String entryName = je.getName();
                Attributes attrs = je.getAttributes();
    
                boolean isDepDescr = false;
                if (attrs != null) {
                    isDepDescr = "true".equalsIgnoreCase(attrs.getValue("SQLJDeploymentDescriptor"));
    
                    if (isDepDescr && deployImageId >= 0) {
                        throw new SQLException(
                                               "Only one SQLJDeploymentDescriptor allowed");
                    }
                }
    
                int nBytes;
                img.reset();
                while ((nBytes = jis.read(buf)) > 0) {
                    img.write(buf, 0, nBytes);
                }
                jis.closeEntry();
    
                stmt.setString(1, entryName);
                stmt.setInt(2, jarId);
                stmt.setBytes(3, img.toByteArray());
                if (stmt.executeUpdate() != 1) {
                    throw new SQLException(
                                           "Jar entry insert did not insert 1 row");
                }
    
                if (isDepDescr) {
                    descIdStmt = SQLUtils.getDefaultConnection().prepareStatement("SELECT entryId FROM sqlj.jar_entry"
                                                                                          + " WHERE jarId = ? AND entryName = ?");
                    descIdStmt.setInt(1, jarId);
                    descIdStmt.setString(2, entryName);
                    rs = descIdStmt.executeQuery();
                    if (!rs.next()) {
                        throw new SQLException(
                                               "Failed to refecth row in sqlj.jar_entry");
                    }
    
                    deployImageId = rs.getInt(1);
                }
            }
            if (deployImageId >= 0) {
                stmt.close();
                stmt = SQLUtils.getDefaultConnection().prepareStatement("UPDATE sqlj.jar_repository SET deploymentDesc = ? WHERE jarId = ?");
                stmt.setInt(1, deployImageId);
                stmt.setInt(2, jarId);
                if (stmt.executeUpdate() != 1) {
                    throw new SQLException(
                                           "Jar repository update did not insert 1 row");
                }
            }
        } catch (IOException e) {
            throw new SQLException("I/O exception reading jar file: "
                                   + e.getMessage());
        } finally {
            if (jis != null) {
                try {
                    jis.close();
                } catch (IOException e) {
                    // ignore
                }
            }
            SQLUtils.close(rs);
            SQLUtils.close(descIdStmt);
            SQLUtils.close(stmt);
        }
    }

    public static void addClassImages(int jarId, String urlString)
                                                                  throws SQLException {
        InputStream urlStream = null;
        boolean wasTrusted = System.getSecurityManager() == s_trustedSecurityManager;
    
        if (wasTrusted) {
            setTrusted(false);
        }
    
        try {
            URL url = new URL(urlString);
            urlStream = url.openStream();
            Backend.addClassImages(jarId, urlStream);
        } catch (IOException e) {
            throw new SQLException("I/O exception reading jar file: "
                                   + e.getMessage());
        } finally {
            if (urlStream != null) {
                try {
                    urlStream.close();
                } catch (IOException e) {
                }
            }
            if (wasTrusted) {
                setTrusted(true);
            }
        }
    }

    public static void clearFunctionCache() {
        synchronized (THREADLOCK) {
            _clearFunctionCache();
        }
    }

    /**
     * Returns the configuration option as read from the Global Unified Config
     * package (GUC).
     * 
     * @param key
     *            The name of the option.
     * @return The value of the option.
     */
    public static String getConfigOption(String key) {
        synchronized (THREADLOCK) {
            return _getConfigOption(key);
        }
    }

    public static synchronized Session getSession() {
        if (s_session == null) {
            s_session = new Session();
        }
        return s_session;
    }

    /**
     * Returns the size of the statement cache.
     * 
     * @return the size of the statement cache.
     */
    public static int getStatementCacheSize() {
        synchronized (THREADLOCK) {
            return _getStatementCacheSize();
        }
    }

    /**
     * Returns <code>true</code> if the backend is awaiting a return from a call
     * into the JVM. This method will only return <code>false</code> when called
     * from a thread other then the main thread and the main thread has returned
     * from the call into the JVM.
     */
    public native static boolean isCallingJava();

    /**
     * Returns the value of the GUC custom variable <code>
     * pljava.release_lingering_savepoints</code>.
     */
    public native static boolean isReleaseLingeringSavepoints();

    private native static void _clearFunctionCache();

    private native static String _getConfigOption(String key);

    private native static int _getStatementCacheSize();

    private native static void _log(int logLevel, String str);

    /**
     * Called when the JVM is first booted and then everytime a switch is made
     * between calling a trusted function versus an untrusted function.
     */
    private static void setTrusted(boolean trusted) {
        s_inSetTrusted = true;
        try {
            Logger log = Logger.getAnonymousLogger();
            if (log.isLoggable(Level.FINE)) {
                log.fine("Using SecurityManager for "
                         + (trusted ? "trusted" : "untrusted") + " language");
            }
            System.setSecurityManager(trusted ? s_trustedSecurityManager
                                             : s_untrustedSecurityManager);
        } finally {
            s_inSetTrusted = false;
        }
    }

    /**
     * Log a message using the internal elog command.
     * 
     * @param logLevel
     *            The log level as defined in {@link ELogHandler}.
     * @param str
     *            The message
     */
    static void log(int logLevel, String str) {
        synchronized (THREADLOCK) {
            _log(logLevel, str);
        }
    }
}
