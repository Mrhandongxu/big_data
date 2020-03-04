/*
 * Program3.java
 *
 * @author Dongxu Han {@literal <dh5913@rit.edu>}
 */

import java.sql.*;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Program to find functional dependency of the table. Modified based on
 * Program2.
 */
public class Program3 {
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
    // List of functional dependencies
    public static List<FuncDependency> fds = new LinkedList<>();
    // All columns
    public static Set<String> R;

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

        @Override
        public String toString() {
            return leftSide + " --> " + rightSide;
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
        // Get all attributes of the table in a set
        R = new HashSet<>(retrieveColumns("new_relation"));
        System.out.println("Retrieve attributes from relation: success.");
        List<String> Rcopy = new ArrayList<>(R);
        detectFD(Rcopy);
        System.out.println("\t" + fds.size() +
            " functional dependencies are found in pruning approach.");
        long end = System.currentTimeMillis();
        System.out.println(
            "\n\tProgram finish in " + df.format((end - start) / 1000) +
                " seconds");
    }

    /**
     * Main function to detect functional dependencies in relation
     *
     * @param columns list of attributes
     */
    public static void detectFD(List<String> columns) {
        List<Set<Set<String>>> allLevel = new LinkedList<>();
        allLevel.add(Collections.emptySet());
        int l = 0;
        Set<Set<String>> level1 = new HashSet<>();
        columns.forEach((c) -> {
            Set<String> single = new HashSet<>();
            single.add(c);
            level1.add(single);
        });
        allLevel.add(level1);
        l++;
        Set<Set<String>> level2 = generateNextLevel(level1);
        allLevel.add(level2);
        l++;
        // starts from the second level since no FD can be generated from
        // singletons of level 1
        while (!allLevel.get(l).isEmpty() && l < 3) {
            // current level to operate
            List<Set<String>> currentLevel = new ArrayList<>(allLevel.get(l));
            List<Set<String>> CPlusX = getCPlusXList(currentLevel);
            // dependency computation
            for (int i = 0; i < CPlusX.size(); i++) {
                Set<String> X = new HashSet<>(currentLevel.get(i));
                X.retainAll(CPlusX.get(i));
                List<String> conjunct = new ArrayList<>(X);
                for (int j = 0; j < conjunct.size(); j++) {
                    Set<String> xCopy = new HashSet<>(X);
                    String A = conjunct.get(j);
                    xCopy.remove(A);
                    if (checkFDExistsInRelation(xCopy, A)) {
                        System.out.println(xCopy + "->" + A);
                        CPlusX.get(i).remove(A);
                        Set<String> RrmX = new HashSet<>(R);
                        RrmX.remove(X);
                        CPlusX.get(i).remove(RrmX);
                    }
                }
            }
            // pruning
            for (int i = 0; i < currentLevel.size(); i++) {
                Set<String> X = currentLevel.get(i);
                if (getCPlusX(new ArrayList<>(X)).isEmpty()) {
                    currentLevel.remove(X);
                }
            }
            Set<Set<String>> nextLevel = generateNextLevel(level1);
            allLevel.add(nextLevel);
            ++l;
        }
    }

    /**
     * Main function to generate next level in lattice.
     *
     * @param level current level
     * @return next level of lattice generated from a given level
     */
    public static Set<Set<String>> generateNextLevel(Set<Set<String>> level) {
        List<Set<String>> dupLevel = new LinkedList<>(level);
        Set<Set<String>> nextLevel = new HashSet<>();
        for (int i = 0; i < level.size(); i++) {
            for (int j = i + 1; j < level.size(); j++) {
                Set<String> newSet = new HashSet<>(dupLevel.get(i));
                newSet.addAll(dupLevel.get(j));
                // to check if subset of new set appear in lower level
                if (validateSet(newSet, level)) {
                    nextLevel.add(newSet);
                }
            }
        }
        return nextLevel;
    }

    /**
     * To check if the subset of new set after removing one element is in the
     * lower level
     *
     * @param newSet new set to check
     * @param level  lower level
     * @return true if all subsets of new set is in the lower level.
     */
    public static boolean validateSet(Set<String> newSet,
        Set<Set<String>> level) {
        AtomicBoolean valid = new AtomicBoolean(true);
        newSet.forEach((s) -> {
            Set<String> dup = new HashSet<>(newSet);
            dup.remove(s);
            if (!level.contains(dup)) {
                valid.set(false);
            }
        });
        return valid.get();
    }

    /**
     * Generate candidate set for all sets in this level
     *
     * @param level current level
     * @return list of candidate sets
     */
    public static List<Set<String>> getCPlusXList(List<Set<String>> level) {
        List<Set<String>> CXs = new ArrayList<>();
        List<Set<String>> levelCopy = new ArrayList<>(level);
        for (int i = 0; i < levelCopy.size(); i++) {
            List<String> X = new ArrayList<>(levelCopy.get(i));
            Set<String> conjunction = getCPlusX(X);
            CXs.add(conjunction);
        }
        return CXs;
    }

    /**
     * Get candidate set C+(X) for a set of X
     *
     * @param x set x
     * @return candidate set
     */
    private static Set<String> getCPlusX(List<String> x) {
        Set<String> conjunction = new HashSet<>(R);
        for (int j = 0; j < x.size(); j++) {
            Set<String> XrmA = new HashSet<>(x);
            XrmA.remove(x.get(j));
            conjunction.retainAll(getCX(XrmA));
        }
        return conjunction;
    }

    /**
     * To get candidate set C(X) for X by C(X)=R\{A in X|X\A -> A holds}
     *
     * @param X set of attributes
     * @return candidate sets of X
     */
    public static Set<String> getCX(Set<String> X) {
        // copy of X to operate on
        Set<String> dupX = new HashSet<>(X);
        // final candidate set, starts from R(set of all attributes)
        Set<String> CX = new HashSet<>(R);
        dupX.forEach((A) -> {
            Set<String> XwithoutA = new HashSet<>(dupX);
            XwithoutA.remove(A);
            if (checkFDExistsInRelation(XwithoutA, A)) {
                CX.remove(A);
            }
        });
        return CX;
    }

    /**
     * Function to check if the FD exists in database
     *
     * @param left  LHS
     * @param right RHS
     * @return if LHS -> RHS holds based on data in relation
     */
    private static boolean checkFDExistsInRelation(Set<String> left,
        String right) {
        Connection con = null;
        boolean isHeld = false;
        try {
            Class.forName(driver);
            con = DriverManager.getConnection(url, user, password);
            Statement stt = con.createStatement();
            if (left == null || left.size() == 0) {
                return false;
            }
            StringJoiner stringJoiner = new StringJoiner(",");
            left.forEach(stringJoiner::add);
            String leftString = stringJoiner.toString();
            String query =
                "SELECT " + leftString + " FROM new_relation " + "GROUP BY " +
                    leftString + " HAVING COUNT (DISTINCT " + right + " ) > 1";
            ResultSet rs = stt.executeQuery(query);
            if (!rs.next()) {
                // If the result set is empty, then the dependency holds
                fds.add(new FuncDependency(left, right));
                isHeld = true;
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
        return isHeld;
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

}
