/*
 * Program1.java
 *
 * @author Dongxu Han {@literal <dh5913@rit.edu>}
 */

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.NumberFormat;

/**
 * Main program to load data into database.
 */
public class Program1 {
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

    // set precision of number format
    static {
        df.setMaximumFractionDigits(2);
    }

    public static void main(String[] args) {
        Connection con = null;
        try {
            Class.forName(driver);
            con = DriverManager.getConnection(url, user, password);
            long start = System.currentTimeMillis();
            Statement stt = con.createStatement();
            // drop table to reset database
            String dropTable = "drop table if exists new_relation;";
            stt.execute(dropTable);
            // create table to
            String createTable =
                "create table new_relation( movieid int, type varchar(50), " +
                    "startyear smallint, runtime int, avgrating numeric(3, " +
                    "1), genreid int, genre varchar(50), memberid int, " +
                    "birthyear smallint, role int);";
            stt.execute(createTable);
            String insertFromQuery =
                "insert into new_relation\n" + "select mv.id,\n" +
                    "       mv.type,\n" + "           mv.startYear,\n" +
                    "           mv.runtime,\n" + "           mv.avgRating,\n" +
                    "           gr.id,\n" + "           gr.name,\n" +
                    "           mb.id,\n" + "           mb.birthyear,\n" +
                    "           amr.role\n" + "from\n" + " (select id,\n" +
                    "         type,\n" + "         startYear,\n" +
                    "         runtime,\n" + "         avgRating\n" +
                    "  from movie as inmv\n" + "  where runtime > 90\n" +
                    "   and not exists\n" + "    (select movie,\n" +
                    "            actor\n" + "     from actor_movie_role\n" +
                    "     where movie = inmv.id\n" + "     group by movie,\n" +
                    "              actor having count(role) > 1)) as mv\n" +
                    "join actor_movie_role as amr on amr.movie = mv.id\n" +
                    "join movie_genre as mg on mv.id = mg.movie\n" +
                    "join genre as gr on gr.id = mg.genre\n" + "join\n" +
                    " (select id,\n" + "         birthyear\n" +
                    "  from member) as mb on mb.id = amr.actor;";
            stt.execute(insertFromQuery);
            String addPK = "alter table new_relation\n" +
                "primary key (movieid, genreid, memberid);";
            stt.execute(addPK);
            String addFKGr = "alter table new_relation\n" +
                "foreign key (genreid) references movie_genre(genre);";
            stt.execute(addFKGr);
            String addFKMb = "alter table new_relation\n" +
                "foreign key (memberid) references member(id);";
            stt.execute(addFKMb);
            String addFKMv = "alter table new_relation\n" +
                "foreign key (movieid) references movie(id);";
            stt.execute(addFKMv);
            long end = System.currentTimeMillis();
            System.out.println(
                "Complete in " + df.format((end - start) / 1000.0) +
                    " seconds.");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (con != null) {
                    con.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
