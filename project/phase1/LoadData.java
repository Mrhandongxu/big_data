import java.io.File;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Scanner;

/**
 * Class to load data in separate files into database.
 *
 * @author Jiaqing Shen
 * @description
 * @date Created in 2020/2/13
 */
public class LoadData {
    // url of postgresql server
    static String url = "jdbc:postgresql://localhost:5432/parking_violations";
    // username
    static String user = "postgres";
    // password
    static String pwd = "123456";

    /**
     * Main function to load vehicle data
     *
     * @param con connection to database
     */
    public static void vehicle(Connection con) {
        try {
            String sql = "";
            sql = "INSERT INTO vehicle(plate,year,make,type, color, " +
                "registState, plateType, expTime, isRegist) values(?,?," +
                "?,?,?,?,?,?,?)";
            PreparedStatement st = con.prepareStatement(sql);
            Scanner sc;
            String filePath =
                "C:\\Users\\Jiaqing Shen\\Desktop\\bigdata\\vehicle.csv";
            sc = new Scanner(new File(filePath));
            long startTime = System.currentTimeMillis();
            int count = 0;

            while (sc.hasNext()) {
                String[] s = sc.nextLine().split(",", -1);
                count++;

                st.setString(1, s[0]);
                st.setInt(2, Integer.parseInt(s[1]));
                st.setString(3, s[2]);
                st.setString(4, s[3]);
                st.setString(5, s[4]);
                st.setString(6, s[5]);
                st.setString(7, s[6]);
                st.setInt(8, Integer.parseInt(s[7]));

                if (s[8] == "1") {
                    st.setBoolean(9, true);
                } else {
                    st.setBoolean(9, false);
                }

                st.addBatch();
            }

            st.executeBatch();
            long endTime = System.currentTimeMillis();
            long usedTime = (endTime - startTime);
            System.out.println(usedTime + "  " + count + "    vehicle ");
            sc.close();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    /**
     * Main function to load ticket data
     *
     * @param con connection to database
     */
    public static void ticket(Connection con) {
        try {
            String sql = "";
            sql =
                "INSERT INTO ticket(ticketId,issueDate,violCode,issueAgency," +
                    "violCounty, violTime, frontOrOppo, lawSect, subSect) " +
                    "values(?,?,?,?,?,?,?,?,?)";
            PreparedStatement st = con.prepareStatement(sql);
            Scanner sc;
            String filePath =
                "C:\\Users\\Jiaqing Shen\\Desktop\\bigdata\\ticket.csv";
            sc = new Scanner(new File(filePath));
            long startTime = System.currentTimeMillis();
            int count = 0;
            DateFormat format = new SimpleDateFormat("MM/dd/yyyy");//日期格式
            while (sc.hasNext()) {
                String[] s = sc.nextLine().split(",", -1);
                count++;

                st.setLong(1, Long.parseLong(s[0]));
                Date dayDateSql = new Date(format.parse(s[1]).getTime());
                st.setDate(2, dayDateSql);//date
                st.setInt(3, Integer.parseInt(s[2]));

                st.setString(4, s[3]);
                st.setString(5, s[4]);
                st.setString(6, s[5]);
                st.setString(7, s[6]);
                st.setString(8, s[7]);
                st.setString(9, s[8]);

                st.addBatch();
            }

            st.executeBatch();
            long endTime = System.currentTimeMillis();
            long usedTime = (endTime - startTime);
            System.out.println(usedTime + "  " + count + "    ticket ");
            sc.close();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    /**
     * Main function to load issuer data
     *
     * @param con connection to database
     */
    public static void issuer(Connection con) {
        try {
            String sql = "";
            sql = "INSERT INTO issuer(issuerId, precinct,squad) values(?,?,?)";
            PreparedStatement st = con.prepareStatement(sql);
            Scanner sc;
            String filePath =
                "C:\\Users\\Jiaqing Shen\\Desktop\\bigdata\\issuer.csv";
            sc = new Scanner(new File(filePath));
            long startTime = System.currentTimeMillis();
            int count = 0;

            while (sc.hasNext()) {
                String[] s = sc.nextLine().split(",", -1);
                count++;

                st.setInt(1, Integer.parseInt(s[0]));
                st.setInt(2, Integer.parseInt(s[1]));
                st.setString(3, (s[2]));

                st.addBatch();
            }

            st.executeBatch();
            long endTime = System.currentTimeMillis();
            long usedTime = (endTime - startTime);
            System.out.println(usedTime + "  " + count + "    issuer ");
            sc.close();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    /**
     * Main function to load relationship data between issuer and ticket
     *
     * @param con connection to database
     */
    public static void issueBy(Connection con) {
        try {
            String sql = "";
            sql =
                "INSERT INTO issueBy(ticketId,issuerId,command) values(?,?,?)";
            PreparedStatement st = con.prepareStatement(sql);
            Scanner sc;
            String filePath =
                "C:\\Users\\Jiaqing Shen\\Desktop\\bigdata\\issueBy.csv";
            sc = new Scanner(new File(filePath));
            long startTime = System.currentTimeMillis();
            int count = 0;

            while (sc.hasNext()) {
                String[] s = sc.nextLine().split(",", -1);
                count++;
                System.out.println(count);
                st.setLong(1, Long.parseLong(s[0]));
                st.setInt(2, Integer.parseInt(s[1]));
                st.setString(3, s[2]);

                st.addBatch();
            }

            st.executeBatch();
            long endTime = System.currentTimeMillis();
            long usedTime = (endTime - startTime);
            System.out.println(usedTime + "  " + count + "    issueBy ");
            sc.close();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    /**
     * Main function to load relationship between ticket and vehicle
     *
     * @param con
     */
    public static void issueTo(Connection con) {
        try {
            String sql = "";
            sql = "INSERT INTO issueTo(ticketId,plate) values(?,?)";
            PreparedStatement st = con.prepareStatement(sql);
            Scanner sc;
            String filePath =
                "C:\\Users\\Jiaqing Shen\\Desktop\\bigdata\\issueTo.csv";
            sc = new Scanner(new File(filePath));
            long startTime = System.currentTimeMillis();
            int count = 0;

            while (sc.hasNext()) {
                String[] s = sc.nextLine().split(",", -1);
                count++;

                st.setLong(1, Long.parseLong(s[0]));
                st.setString(2, s[1]);

                st.addBatch();
            }

            st.executeBatch();
            long endTime = System.currentTimeMillis();
            long usedTime = (endTime - startTime);
            System.out.println(usedTime + "  " + count + "    issueTo ");
            sc.close();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    /**
     * Main function to load several files into database
     *
     * @param args commandline arguments
     */
    public static void main(String[] args) {
        try {
            Connection con = DriverManager.getConnection(url, user, pwd);
            System.out.println("Connect success");
            Class.forName("org.postgresql.Driver");

            con.setAutoCommit(false);
            vehicle(con);
            ticket(con);
            issuer(con);
            issueBy(con);
            issueTo(con);

            con.commit();
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
