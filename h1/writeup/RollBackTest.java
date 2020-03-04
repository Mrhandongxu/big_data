/*
 * RollBackTest.java
 *
 * @author Dongxu Han {@literal <dh5913@rit.edu>}
 */

import java.sql.*;

/**
 * This class is to test whether there is an effect on database, when inserting
 * 3 rows with the second containing an error.
 */
public class RollBackTest {
    // server url
    private static String url = "jdbc:postgresql://localhost:5432/imdb_dx";
    // username of database
    private static String user = "dongxu";
    // password of username
    private static String password = "199710";
    // jdbc driver
    private static String driver = "org.postgresql.Driver";

    /**
     * Main program to make test. Firstly we insert a record. Then the second
     * sql would try to insert a record with duplicate key. Then check if the
     * first sql take effects.
     *
     * @param args commandline arguments
     */
    public static void main(String[] args) {
        Connection con = null;
        try {
            Class.forName(driver);
            con = DriverManager.getConnection(url, user, password);
            // set autocommit to false to avoid accidentally commitment
            con.setAutoCommit(false);
            Statement stmt = con.createStatement();
            String sql1 =
                "insert into person values(9993720, 'dongxu', 1997, null);";
            // this sql would create an error since it try to insert a row
            // with an existing primary key
            String sql2 =
                "insert into person values(9993720, 'error', 1909, 1992);";
            String sql3 =
                "insert into person values(9993721, 'noexecute', 1910, 1970);";
            try {
                stmt.execute(sql1);
                System.out.println("sql1 executed.");
                // sql2 would generate an error and this transaction would
                // rollback
                stmt.execute(sql2);
                stmt.execute(sql3);
                con.commit();
            } catch (Exception e) {
                System.out.println("Exception occur: " + e.getMessage());
                if (con != null) {
                    con.rollback();
                    System.out.println("Rollback occurs.");
                }
            }
            // here we try to select the record which should be inserted by
            // sql1, but be canceled due to error in sql2
            String query = "select * from person where pconst = 9993720";
            ResultSet rs = stmt.executeQuery(query);
            if (rs.next() == false) {
                System.out.println("Test pass. No effect on database.");
            }
        } catch (Exception e) {
            System.out.println(e.getClass() + ": " + e.getMessage());
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
        }
    }
}
