/*
 * Program2.java
 *
 * @author Dongxu Han {@literal <dh5913@rit.edu>}
 */

import java.sql.*;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;

/**
 * Program to find functional dependency of the table.
 */
public class Program2 {
    // url of postgresql server
    private static String url = "jdbc:postgresql://localhost:5432/postgres";
    // username of postgresql
    private static String user = "postgres";
    // password of user
    private static String password = "123456";
    // jdbc driver
    private static String driver = "org.postgresql.Driver";
    // float number format as 1 decimal
    private static DecimalFormat df =
        (DecimalFormat) NumberFormat.getInstance();
    // functional dependency map, If A -> b holds, then A is key and b is value
    public static List<FuncDependency> funcDependency = new LinkedList<>();

    /**
     * Data structure for functional dependency. The right side depends on left
     * side
     */
    public static class FuncDependency {
        public Set<String> leftSide;
        public String rightSide;

        public FuncDependency(Set<String> leftSide, String rightSide) {
            this.leftSide = leftSide;
            this.rightSide = rightSide;
        }
    }

    /**
     * Main program to find functional dependency in the new relation table
     * created in question 1. In a naive approach, we retrieve all columns of
     * the tables. Then we got all combinations of these columns. Then for each
     * column, we create a thread to determine if the column depends on some
     * combinations, which are sets of attributes of the table. At last, we
     * wait for all threads to finish and report the time.
     *
     * @param args commandline arguments(ignored)
     */
    public static void main(String[] args) {
        System.out.println("Program started.");
        long start = System.currentTimeMillis();
        // put all attributes of the table in a list
        List<String> columns = retrieveColumns("new_relation");
        System.out.println("Retrieve attributes from relation: success.");
        // generate combinations of
        Set<Set<String>> combinations = getCombinations(columns);
        System.out.println("Generate combinations: success.");
        List<Thread> threads = new LinkedList<>();
        for (String column : columns) {
            // for each attributes, find the set of attributes it depends on
            Thread thread =
                new Thread(() -> findFuncDepe(combinations, column));
            threads.add(thread);
            thread.start();
        }
        System.out.println("Created threads to find functional dependencies.");
        for (Thread thread : threads) {
            try {
                // wait for all threads to finish
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        long end = System.currentTimeMillis();
        System.out.println(
            "\n\tProgram finish in " + df.format((end - start) / 1000) +
                " seconds");
        System.out.println("\n\t" + funcDependency.size() +
            " functional dependencies are found in naive approach.");
    }

    /**
     * Function to check if one attributes depends on some sets of attributes.
     * The size of the set may be one or more.
     *
     * @param potentialKeys set of subsets of attributes of the table
     * @param attribute     attribute to check
     */
    public static void findFuncDepe(Set<Set<String>> potentialKeys,
        String attribute) {
        Connection con = null;
        // to store functional dependencies if available.
        List<FuncDependency> funcDep = new LinkedList<>();
        try {
            Class.forName(driver);
            con = DriverManager.getConnection(url, user, password);
            Statement stt = con.createStatement();
            for (Set<String> currentSet : potentialKeys) {
                String keyString = currentSet.toString();
                keyString = keyString.substring(1, keyString.length() - 1);
                if (currentSet.contains(attribute)) {
                    // according to reflexivity rule, if A is a set of
                    // attributes and b belongs to A, then A -> b holds.
                    // Then just skip this.
                    funcDep.add(new FuncDependency(currentSet, attribute));
                } else {
                    // This is the most costly part to do.
                    // this query is to check if one attributes depends on a
                    // set of attributes.
                    // For example, to check if b depends on a, then the query
                    // is
                    // **********
                    // select a from relation_name
                    // group by a
                    // having count(distinct b) > 1;
                    // **********
                    // If this query returns anything, that means for one
                    // instance of a, there exists multiple values of b
                    // corresponding to a. This violate the definition of
                    // functional dependence.
                    // If the query returns nothing, that means for every
                    // instance of a, there exists distinct value of b for a.
                    // This means a -> b holds.
                    String query =
                        "SELECT " + keyString + " FROM new_relation " +
                            "GROUP BY " + keyString +
                            " HAVING COUNT (DISTINCT " + attribute + " ) > 1";
                    ResultSet rs = stt.executeQuery(query);
                    if (!rs.next()) {
                        // If the result set is empty, then the dependency
                        // holds
                        funcDep.add(new FuncDependency(currentSet, attribute));
                    }
                    // otherwise not holds
                }
            }
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        } finally {
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        // Synchronize to avoid error when multiple threads operate this at
        // the same time.
        synchronized (funcDependency) {
            funcDependency.addAll(funcDep);
        }
    }

    /**
     * Function to retrieve all attributes of the relation.
     *
     * @param relation relation(table) name
     * @return list of attributes
     */
    public static List<String> retrieveColumns(String relation) {
        Connection con = null;
        List<String> columns = new ArrayList<>();
        try {
            Class.forName(driver);
            con = DriverManager.getConnection(url, user, password);
            PreparedStatement stt = con.prepareStatement(
                "SELECT COLUMN_NAME\n" + "FROM information_schema.columns\n" +
                    "WHERE TABLE_NAME = ?;");
            stt.setString(1, relation);
            ResultSet rs = stt.executeQuery();
            while (rs.next()) {
                columns.add(rs.getString(1));
            }
            return columns;
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        } finally {
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return columns;
    }

    /**
     * Function to generate power set of a list
     *
     * @param elements list of elements
     * @param <T>      generic type
     * @return set of subsets of a list
     */
    public static <T> Set<Set<T>> getCombinations(List<T> elements) {
        Set<Set<T>> result = new HashSet<>();
        int n = (int) Math.pow(2, elements.size());
        // consider i as binary notation
        // if j(th) bit of i is 1, that means the j(th) of elements is in
        // current set
        for (int i = 1; i < n; i++) {
            Set<T> set = new HashSet<>();
            for (int j = 0; j < elements.size(); j++) {
                // 1 is ...00001
                // at each loop, it will increase by 1 and become ...010
                // , ..0100
                // conjunct it with i to get j(th) bit of i, which is 1
                if ((i & (1 << j)) != 0) {
                    set.add(elements.get(j));
                }
            }
            result.add(set);
        }
        return result;
    }
}
