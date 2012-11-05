/*
 * Copyright (c) 2004, 2005 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.example;

import java.lang.reflect.InvocationTargetException;
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
public class MetaDataInts implements ResultSetProvider {
    public static ResultSetProvider getDatabaseMetaDataInts()
                                                             throws SQLException {
        try {
            return new MetaDataInts();
        } catch (SQLException e) {
            throw new SQLException("Error reading DatabaseMetaData",
                                   e.getMessage());
        }
    }

    String[]  methodNames;

    Integer[] methodResults;

    public MetaDataInts() throws SQLException {
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
        Integer result = null;
        ArrayList<String> mn = new ArrayList<String>();
        ArrayList<Integer> mr = new ArrayList<Integer>();

        for (Method element : m) {
            prototype = element.getParameterTypes();
            if (prototype.length > 0) {
                continue;
            }

            returntype = element.getReturnType();
            if (!returntype.equals(int.class)) {
                continue;
            }

            try {
                result = (Integer) element.invoke(md, args);
            } catch (InvocationTargetException e) {
                log.info("Method: " + element.getName() + " => "
                         + e.getTargetException().getMessage());
                result = new Integer(-1);
            } catch (Exception e) {
                log.info("Method: " + element.getName() + " => "
                         + e.getMessage());
                result = new Integer(-1);
            }

            mn.add(element.getName());
            mr.add(result);
        }

        methodNames = mn.toArray(new String[mn.size()]);
        methodResults = mr.toArray(new Integer[mr.size()]);
    }

    public boolean assignRowValues(ResultSet receiver, int currentRow)
                                                                      throws SQLException {
        if (currentRow < methodNames.length) {
            receiver.updateString(1, methodNames[currentRow]);
            receiver.updateInt(2, methodResults[currentRow].intValue());
            return true;
        }
        return false;
    }

    public void close() {
    }
}
