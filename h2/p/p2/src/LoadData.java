/*
 * LoadData.java
 *
 * @author Dongxu Han {@literal <dh5913@rit.edu>}
 */

import java.io.*;
import java.sql.*;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class to populate data from file into database.
 * In general, this class has five threads to handle different files and tables.
 * The order threads running are movie, member, then other tables that have
 * some attributes reference to movie or member. Even though I disable foreign
 * key, it is the logical constrain on order of loading data.
 * All threads are doing thing in same steps. That is reading a line from file,
 * picking useful data and forming sql statement. Then use batch update or
 * insert to improve performance.
 * Since there are some duplicate data in file, I handle this by "on conflict
 * do nothing" to avoid error when inserting duplicate data.
 * For genre, since it doesn't have too much data, I use a map to store genre
 * name and corresponding id.
 *
 */
public class LoadData {
    // url of postgresql server
    private static String url = "jdbc:postgresql://localhost:5432/postgres";
    // username of postgresql
    private static String user = "postgres";
    // password of user
    private static String password = "123456";
    // jdbc driver
    private static String driver = "org.postgresql.Driver";
    // file directory
    private static String directory =
        "/Users/hdx/Documents/coursework/grad/CS620_Big " +
            "Data/homework/imdbdata/";
    // float number format as 1 decimal
    private static DecimalFormat df =
        (DecimalFormat) NumberFormat.getInstance();

    // set precision of number format
    static {
        df.setMaximumFractionDigits(2);
    }

    public static void main(String[] args) {
        try {
            long start = System.currentTimeMillis();

            Thread mvGenre = new Thread(() -> loadMovieGenre());
            Thread wrDrct = new Thread(() -> loadWriteDirect());
            Thread actProdRole = new Thread(() -> loadActProduceRole());
            Thread rating = new Thread(() -> loadRatings());
            Thread member = new Thread(() -> loadMember());
            // start all threads
            member.start();
            mvGenre.start();
            wrDrct.start();
            actProdRole.start();
            // wait for all threads to finish
            mvGenre.join();
            // update of rating should be start after movie data loaded
            rating.start();
            member.join();
            wrDrct.join();
            actProdRole.join();
            rating.join();
            long end = System.currentTimeMillis();
            System.out.println("Finish loading all data within " +
                df.format((end - start) / 1000.0 / 60) + " minutes.");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Function to update ratings in movie table. This function will be execute
     * after main information of movie being loaded in database.
     */
    private static void loadRatings() {
        Connection con = null;
        try {
            Class.forName(driver);
            con = DriverManager.getConnection(url, user, password);
            con.setAutoCommit(false);
            // sql for update rating
            PreparedStatement ratStt = con.prepareStatement(
                "update movie set avgRating=?, numVotes=? where id=?");
            BufferedReader reader = new BufferedReader(new InputStreamReader(
                new FileInputStream(directory + "title.ratings.tsv")));
            reader.readLine();
            String line;
            // count for sql in batches
            int count = 0;
            long start = System.currentTimeMillis();
            while ((line = reader.readLine()) != null) {
                String[] values = line.trim().replace("'", "''").split("\t");
                // id
                ratStt.setInt(3, Integer.parseInt(values[0].substring(2)));
                // avgRating
                if (values[1].equals("\\N")) {
                    ratStt.setNull(1, Types.REAL);
                } else {
                    ratStt.setFloat(1, Float.parseFloat(values[1]));
                }
                // numVotes
                if (values[2].equals("\\N")) {
                    ratStt.setNull(2, Types.INTEGER);
                } else {
                    ratStt.setInt(2, Integer.parseInt(values[2]));
                }
                ratStt.addBatch();
                if (++count == 1000) {
                    ratStt.executeBatch();
                    count = 0;
                }
            }
            // execute remaining sql
            if (count != 0) {
                ratStt.executeBatch();
            }
            con.commit();
            long end = System.currentTimeMillis();
            System.out.println("Thread update ratings within " +
                df.format((end - start) / 1000.0 / 60) + " minutes.");
            con.close();
        } catch (Exception e) {
            e.printStackTrace();
            try {
                if (con != null) {
                    con.rollback();
                    con.close();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }

    /**
     * Function to load relation between movie and actor, movie and producer,
     * role, actor and movie and role. Since we may have duplicate tuples if
     * picking up from original data, I use "on conflict do nothing" to ignore
     * operation. This
     */
    private static void loadActProduceRole() {
        Connection con = null;
        try {
            Class.forName(driver);
            con = DriverManager.getConnection(url, user, password);
            con.setAutoCommit(false);
            // sql for movie_actor table
            PreparedStatement actStt = con.prepareStatement(
                "insert into movie_actor values(?,?) on conflict(actor, " +
                    "movie) do nothing");
            // sql for movie_producer table
            PreparedStatement prdStt = con.prepareStatement(
                "insert into movie_producer values(?,?) on conflict" +
                    "(producer, movie) do nothing");
            // sql for role table
            PreparedStatement rlStt =
                con.prepareStatement("insert into role values(?,?)");
            int roleId = 0; // id for a single role
            // sql for actor_movie_role table
            PreparedStatement amrStt = con.prepareStatement(
                "insert into actor_movie_role values(?,?,?)");

            BufferedReader reader = new BufferedReader(new InputStreamReader(
                new FileInputStream(directory + "title.principals.tsv")));
            reader.readLine();
            // pattern to pick content between two double quotation mark
            Pattern pattern = Pattern.compile("\"(.*?)\"");
            int count = 0;
            long start = System.currentTimeMillis();
            String line;
            while ((line = reader.readLine()) != null) {
                String[] values = line.trim().replace("'", "''").split("\t");
                // as for an actor tuple, we should add
                // relation between movie and actor, the role, if exists,
                // and relation among actor, movie and role
                if (values[3].equals("actor")) {
                    // actor
                    actStt.setInt(1, Integer.parseInt(values[2].substring(2)));
                    // movie
                    actStt.setInt(2, Integer.parseInt(values[0].substring(2)));
                    actStt.addBatch();
                    if (!values[5].equals("\\N")) {
                        Matcher matcher = pattern.matcher(values[5]);
                        // for each role string
                        while (matcher.find()) {
                            // sql for a single role
                            // id
                            rlStt.setInt(1, ++roleId);
                            // role name
                            rlStt.setString(2,
                                matcher.group().replace("\"", ""));
                            rlStt.addBatch();
                            // adding relationship among role movie and actor
                            // actor
                            amrStt.setInt(1,
                                Integer.parseInt(values[2].substring(2)));
                            // movie
                            amrStt.setInt(2,
                                Integer.parseInt(values[0].substring(2)));
                            // role id
                            amrStt.setInt(3, roleId);
                            amrStt.addBatch();
                        }
                    }
                }
                // add producer
                if (values[3].equals("producer")) {
                    // producer
                    prdStt.setInt(1, Integer.parseInt(values[2].substring(2)));
                    // movie
                    prdStt.setInt(2, Integer.parseInt(values[0].substring(2)));
                    prdStt.addBatch();
                }
                if (++count == 1000) {
                    actStt.executeBatch();
                    prdStt.executeBatch();
                    amrStt.executeBatch();
                    rlStt.executeBatch();
                    count = 0;
                }
            }
            if (count != 0) {
                actStt.executeBatch();
                prdStt.executeBatch();
                amrStt.executeBatch();
                rlStt.executeBatch();
            }
            con.commit();
            long end = System.currentTimeMillis();
            System.out.println(
                "Thread loads movie_actor, movie_producer, role, " +
                    "actor_movie_role within " +
                    df.format((end - start) / 1000.0 / 60) + " minutes.");
            con.close();
        } catch (Exception e) {
            e.printStackTrace();
            try {
                if (con != null) {
                    con.rollback();
                    con.close();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }

    /**
     * Function to load relation between movie and writer, movie and director
     */
    private static void loadWriteDirect() {
        Connection con = null;
        try {
            Class.forName(driver);
            con = DriverManager.getConnection(url, user, password);
            con.setAutoCommit(false);
            // statement for sql populating movie_director
            PreparedStatement dirctStt = con.prepareStatement(
                "insert into movie_director values(?,?) on conflict" +
                    "(director, movie) do nothing");
            // statement for sql populating movie_writer
            PreparedStatement wrtStt = con.prepareStatement(
                "insert into movie_writer values(?,?) on conflict(writer, " +
                    "movie) do nothing");

            BufferedReader reader = new BufferedReader(new InputStreamReader(
                new FileInputStream(directory + "title.crew.tsv")));
            reader.readLine();
            String line;
            int count = 0;
            long start = System.currentTimeMillis();
            while ((line = reader.readLine()) != null) {
                String[] values = line.trim().replace("'", "''").split("\t");
                if (!values[1].equals("\\N")) {
                    // for each director, it is a valid tuple
                    for (String director : values[1].split(",")) {
                        // director id
                        dirctStt.setInt(1,
                            Integer.parseInt(director.substring(2)));
                        // movie id
                        dirctStt.setInt(2,
                            Integer.parseInt(values[0].substring(2)));
                        dirctStt.addBatch();
                    }
                }
                if (!values[2].equals("\\N")) {
                    for (String writer : values[2].split(",")) {
                        // writer id
                        wrtStt
                            .setInt(1, Integer.parseInt(writer.substring(2)));
                        // movie id
                        wrtStt.setInt(2,
                            Integer.parseInt(values[0].substring(2)));
                        wrtStt.addBatch();
                    }
                }
                if (++count == 1000) {
                    wrtStt.executeBatch();
                    dirctStt.executeBatch();
                    count = 0;
                }
            }
            if (count != 0) {// finish some remaining sql
                wrtStt.executeBatch();
                dirctStt.executeBatch();
            }
            con.commit();
            long end = System.currentTimeMillis();
            System.out.println("Thread loads write and direct within " +
                df.format((end - start) / 1000.0 / 60) + " minutes.");
            con.close();
        } catch (Exception e) {
            e.printStackTrace();
            try {
                if (con != null) {
                    con.rollback();
                    con.close();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }

    /**
     * Function to load data to member table
     */
    private static void loadMember() {
        Connection con = null;
        try {
            Class.forName(driver);
            con = DriverManager.getConnection(url, user, password);
            con.setAutoCommit(false);
            // sql of inserting data to member
            PreparedStatement memStt =
                con.prepareStatement("insert into member values(?,?,?,?)");

            BufferedReader reader = new BufferedReader(new InputStreamReader(
                new FileInputStream(directory + "name.basics.tsv")));
            // skip the first line
            reader.readLine();
            String line;
            int count = 0;
            long start = System.currentTimeMillis();
            while ((line = reader.readLine()) != null) {
                String[] values = line.trim().replace("'", "''").split("\t");
                // id
                memStt.setInt(1, Integer.parseInt(values[0].substring(2)));
                // name
                memStt.setString(2,
                    values[1].equals("\\N") ? "null" : values[1]);
                // brith year
                if (values[2].equals("\\N")) {
                    memStt.setNull(3, Types.INTEGER);
                } else {
                    memStt.setInt(3, Integer.parseInt(values[2]));
                }
                // death year
                if (values[3].equals("\\N")) {
                    memStt.setNull(4, Types.INTEGER);
                } else {
                    memStt.setInt(4, Integer.parseInt(values[3]));
                }
                memStt.addBatch();
                if (++count == 1000) {
                    memStt.executeBatch();
                    count = 0;
                }
            }
            if (count != 0) {
                memStt.executeBatch();
            }
            con.commit();
            long end = System.currentTimeMillis();
            System.out.println("Thread loads member within " +
                df.format((end - start) / 1000.0 / 60) + " minutes");
            con.close();
        } catch (Exception e) {
            e.printStackTrace();
            try {
                if (con != null) {
                    con.rollback();
                    con.close();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }

    /**
     * Function to load data to movie, genre, and movie_genre table.
     */
    private static void loadMovieGenre() {
        Connection con = null;
        try {
            Class.forName(driver);
            con = DriverManager.getConnection(url, user, password);
            con.setAutoCommit(false);
            // create prepared statement for three tables
            // movie
            PreparedStatement mvStt = con.prepareStatement(
                "insert into movie values(?,?,?,?,?,?,null,null)");
            // genre
            PreparedStatement gnStt =
                con.prepareStatement("insert into genre values(?,?)");
            // movie_genre
            PreparedStatement mgStt = con.prepareStatement(
                "insert into movie_genre values(?,?) on conflict(genre, " +
                    "movie) do nothing");

            BufferedReader titleReader = new BufferedReader(
                new InputStreamReader(
                    new FileInputStream(directory + "title.basics.tsv")));
            titleReader.readLine();
            String titleLine;
            long start = System.currentTimeMillis();
            int count = 0; // count for tuples processed
            int genreId = 0; // id for new genre
            // map to store known genres, key: genre name, value: genre id
            Map<String, Integer> genres = new HashMap<>();
            while ((titleLine = titleReader.readLine()) != null) {
                String[] ttValues =
                    titleLine.trim().replace("'", "''").split("\t");
                // id
                mvStt.setInt(1, Integer.parseInt(ttValues[0].substring(2)));
                // type
                mvStt.setString(2, ttValues[1]);
                // title
                mvStt.setString(3, ttValues[3]);
                // startYear
                if (ttValues[5].equals("\\N")) {
                    mvStt.setNull(4, Types.INTEGER);
                } else {
                    mvStt.setInt(4, Integer.parseInt(ttValues[5]));
                }
                // endYear
                if (ttValues[6].equals("\\N")) {
                    mvStt.setNull(5, Types.INTEGER);
                } else {
                    mvStt.setInt(5, Integer.parseInt(ttValues[6]));
                }
                // runtime
                if (ttValues[7].equals("\\N")) {
                    mvStt.setNull(6, Types.INTEGER);
                } else {
                    mvStt.setInt(6, Integer.parseInt(ttValues[7]));
                }

                // genres
                if (!ttValues[8].equals("\\N")) { // if not null
                    for (String tt : ttValues[8].split(",")) {
                        if (!genres.containsKey(tt)) {
                            // if the genre hasn't been recorded
                            // add it to map with its name and id
                            genres.put(tt, ++genreId);
                            gnStt.setInt(1, genreId);
                            gnStt.setString(2, tt);
                            gnStt.addBatch();
                        }
                        // for each genre, get the id of genre
                        // and relationship between genre and movie to database
                        mgStt.setInt(1, genres.get(tt));
                        mgStt.setInt(2,
                            Integer.parseInt(ttValues[0].substring(2)));
                        mgStt.addBatch();
                    }
                }
                mvStt.addBatch();
                ++count;
                // execute every 1000 instances
                if (count == 1000) {
                    mvStt.executeBatch();
                    gnStt.executeBatch();
                    mgStt.executeBatch();
                    count = 0;
                }
            }
            if (count != 0) { // some sql may not execute until EOF
                mvStt.executeBatch();
                gnStt.executeBatch();
                mgStt.executeBatch();
            }
            con.commit();
            long end = System.currentTimeMillis();
            // report time
            System.out.println(
                "Thread loads movie, genre, movie_genre within " +
                    df.format((end - start) / 1000.0 / 60) + " minutes.");
            con.close();
        } catch (Exception e) {
            e.printStackTrace();
            try {
                if (con != null) {
                    con.rollback();
                    con.close();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }
}
