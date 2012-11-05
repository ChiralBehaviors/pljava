/*
 * Copyright (c) 2004, 2005 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.example;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.logging.Logger;

import org.postgresql.pljava.ResultSetProvider;

/**
 * @author Filip Hrbek
 */
public class MetaDataStrings implements ResultSetProvider {
    public static ResultSetProvider getDatabaseMetaDataStrings()
                                                                throws SQLException {
        try {
            return new MetaDataStrings();
        } catch (SQLException e) {
            throw new SQLException("Error reading DatabaseMetaData",
                                   e.getMessage());
        }
    }

    String[] methodNames;

    String[] methodResults;

    public MetaDataStrings() throws SQLException {
        Logger log = Logger.getAnonymousLogger();

        class MethodComparator implements Comparator<Object> {
            public int compare(Object a, Object b) {
                return ((Method) a).getName().compareTo(((Method) b).getName());
            }
        }

        Connection conn = DriverManager.getConnection("jdbc:default:connection");
        DatabaseMetaData md = conn.getMetaData();
        Method[] m = DatabaseMetaData.class.getMethods();
        Arrays.sort(m, new MethodComparator());
        Class<?> prototype[];
        Class<?> returntype;
        Object[] args = new Object[0];
        String result = null;
        ArrayList<String> mn = new ArrayList<String>();
        ArrayList<String> mr = new ArrayList<String>();

        for (Method element : m) {
            prototype = element.getParameterTypes();
            if (prototype.length > 0) {
                continue;
            }

            returntype = element.getReturnType();
            if (!returntype.equals(String.class)) {
                continue;
            }

            try {
                result = (String) element.invoke(md, args);
                log.info("Method: " + element.getName() + " => Success");
            } catch (Exception e) {
                log.info("Method: " + element.getName() + " => "
                         + e.getMessage());
            }

            mn.add(element.getName());
            mr.add(result);
        }

        methodNames = mn.toArray(new String[0]);
        methodResults = mr.toArray(new String[0]);
    }

    public boolean assignRowValues(ResultSet receiver, int currentRow)
                                                                      throws SQLException {
        if (currentRow < methodNames.length) {
            receiver.updateString(1, methodNames[currentRow]);
            receiver.updateString(2, methodResults[currentRow]);
            return true;
        }
        return false;
    }

    public void close() {
    }
}
