/*
 * QueryData.java
 *
 * @author Dongxu Han {@literal <dh5913@rit.edu>}
 */

import java.sql.*;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class QueryData {
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

    /**
     * Main program to create threads to do query.
     *
     * @param args commandline arguments
     */
    public static void main(String[] args) {
        //Thread thread1 = new Thread(() -> query1());
        //thread1.start();
        //Thread thread2 = new Thread(() -> query2());
        //thread2.start();
        //Thread thread3 = new Thread(() -> query3());
        //thread3.start();
        //Thread thread4 = new Thread(() -> query4());
        //thread4.start();
        Thread thread5 = new Thread(() -> query5());
        thread5.start();

    }

    /**
     * Function to query Number of invalid Movie_Actor relationships with
     * respect to roles.
     */
    private static void query1() {
        Connection con = null;
        try {
            Class.forName(driver);
            con = DriverManager.getConnection(url, user, password);
            long start = System.currentTimeMillis();
            Statement stt = con.createStatement();
            ResultSet res = stt.executeQuery(
                "select count(*)\n" + "from movie_actor\n" +
                    "where not exists (select actor from actor_movie_role\n" +
                    "where movie_actor.actor = actor_movie_role.actor);");
            long end = System.currentTimeMillis();
            res.next();
            // get count result of query
            int count = res.getInt("count");
            System.out.println(
                "Finish retrieve number of invalid Movie_Actor" +
                    " relationships with respect to roles\n in " +
                    getTime(end - start) + " seconds");
            System.out.println("Count: " + count);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Function to query number of invalid Movie_Actor relationships with
     * respect to roles.
     */
    private static void query2() {
        Connection con = null;
        try {
            Class.forName(driver);
            con = DriverManager.getConnection(url, user, password);
            long start = System.currentTimeMillis();
            Statement stt = con.createStatement();
            ResultSet res = stt.executeQuery(
                "select distinct mb.name from movie_actor as ma \n" +
                    "join (select id, name from member where deathYear is " +
                    "null and name like 'Phi%') as mb\n" +
                    "on ma.actor = mb.id\n" +
                    "where not exists(select * from movie where startYear = " +
                    "2014 and ma.movie = movie.id);");
            // get count result of query
            long end = System.currentTimeMillis();
            List<String> names = new LinkedList<>();
            while (res.next()) {
                names.add(res.getString("name"));
            }
            System.out.println(
                "Finish retrieve alive actors whose name starts with “Phi” " +
                    "and did not participate in any movie in 2014\n in " +
                    getTime(end - start) + " seconds");
            System.out.println("number of results: " + names.size());
            System.out.println("Few examples are shown below.");
            for (int i = 0; i < 5; i++) {
                System.out.println(names.get(i));
            }
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Function to query producers who have produced the most talk shows in
     * 2017 and whose name contains “Gill”.
     */
    private static void query3() {
        Connection con = null;
        try {
            Class.forName(driver);
            con = DriverManager.getConnection(url, user, password);
            long start = System.currentTimeMillis();
            Statement stt = con.createStatement();
            ResultSet res = stt.executeQuery(
                "select mm.name, count(mp.movie) as ct from movie_producer " +
                    "as mp, member as mm\n" + "where \n" +
                    "exists(select * from movie where startYear = 2017 and " +
                    "movie.id = mp.movie) \n" + "and \n" +
                    "exists(select * from member where name like '%Gill%' " +
                    "and member.id = mp.producer\n" +
                    "and mp.producer = mm.id)\n" + "group by mm.name\n" +
                    "order by ct desc\n" + "limit 1;");
            // get count result of query
            long end = System.currentTimeMillis();
            res.next();
            System.out.println(
                "Finish retrieve producers who have produced the most talk " +
                    "shows in 2017 and whose name contains “Gill” in 2014\n " +
                    "in " + getTime(end - start) + " seconds");
            System.out.println("result:\n name: " + res.getString(1) +
                ";number of talk show produced: " + res.getInt(2));
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Function to query alive producers with the greatest number of long-run
     * movies produced.
     */
    private static void query4() {
        Connection con = null;
        try {
            Class.forName(driver);
            con = DriverManager.getConnection(url, user, password);
            long start = System.currentTimeMillis();
            Statement stt = con.createStatement();
            ResultSet res = stt.executeQuery(
                "select mm.name, count(movie) as ct from movie_producer as " +
                    "mp, member as mm\n" +
                    "where exists (select * from movie where runtime > 120 " +
                    "and movie.id = mp.movie)\n" +
                    "and exists (select * from member where deathYear is " +
                    "null and mm.id = mp.producer)\n" +
                    "and mp.producer = mm.id\n" + "group by mm.name\n" +
                    "order by ct desc\n" + "limit 1;");
            // get count result of query
            long end = System.currentTimeMillis();
            res.next();
            System.out.println(
                "Finish retrieve alive producers with the greatest number of" +
                    " long-run movies produced\n" + "in " +
                    getTime(end - start) + " seconds");
            System.out.println("result:\n name: " + res.getString(1) +
                ";number of movie produced: " + res.getInt(2));
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Function to query alive actors who have portrayed Jesus Christ (look for
     * both words independently)
     */
    private static void query5() {
        Connection con = null;
        try {
            Class.forName(driver);
            con = DriverManager.getConnection(url, user, password);
            long start = System.currentTimeMillis();
            Statement stt = con.createStatement();
            ResultSet res = stt.executeQuery(
                "select distinct mm.name, rl.role\n" +
                    "from actor_movie_role as amr join member as mm \n" +
                    "on mm.id = amr.actor\n" + "join role as rl \n" +
                    "on rl.id = amr.role\n" +
                    "where (rl.role like '%Jesus%' or rl.role like " +
                    "'%Christ%') and mm.deathYear is null;");
            // get count result of query
            long end = System.currentTimeMillis();
            Map<String, String> map = new HashMap<>();
            while (res.next()) {
                map.put(res.getString(1), res.getString(2));
            }
            System.out.println(
                "Finish retrieve alive actors who have portrayed Jesus " +
                    "Christ (look for both words independently)\n in " +
                    getTime(end - start) + " seconds");
            System.out.println("result:\n number of results: " + map.size());
            System.out.println("Few examples are shown below.");
            int i = 0;
            for (String key : map.keySet()) {
                System.out.println(key + ", " + map.get(key));
                if (++ i == 5) {
                    break;
                }
            }
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Convert ms to seconds
     *
     * @param runTime runtime in ms
     * @return runtime in seconds
     */
    private static String getTime(long runTime) {
        return df.format(runTime / 1000.0);
    }
}
