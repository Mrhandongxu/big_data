/*
 * Imdb.java
 *
 * @author Dongxu Han {@literal <dh5913@rit.edu>}
 */

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * The class to load dataset of imdb to postgresql. This class depends on
 * PostgreSQL driver to connect with databases.
 * Usage:
 * step1. javac -cp ".:./postgresql-42.2.9.jar" Imdb.java;
 * step2. java -cp ".:./postgresql-42.2.9.jar" Imdb
 * Note: "./postgresql-42.2.9.jar" may be replaced by local path of jar on
 * different machine. And file path should also be changed while running on
 * different machine.
 */
public class Imdb {
    // url of postgresql server
    private static String url = "jdbc:postgresql://localhost:5432/imdb_dx";
    // username of postgresql
    private static String user = "dongxu";
    // password of user
    private static String password = "199710";
    // jdbc driver
    private static String driver = "org.postgresql.Driver";
    // map containing path data files
    private static Map<String, String> filepath = new HashMap<>();

    // initialize filepath map
    static {
        filepath.put("title_ratings", "title.ratings.tsv");
        filepath.put("title_principals", "title.principals.tsv");
        filepath.put("title_crew", "title.crew.tsv");
        filepath.put("title_basics", "title.basics.tsv");
        filepath.put("name_basics", "name.basics.tsv");
    }

    // float number format as 1 decimal
    private static DecimalFormat df =
        (DecimalFormat) NumberFormat.getInstance();

    /**
     * Main program. In main function, start several threads for different
     * dataset and create mutiple connections to database server. Then these
     * threads work simultaneously to load data into database.
     *
     * @param args
     */
    public static void main(String[] args) {
        try {
            long start = System.currentTimeMillis();
            df.setMaximumFractionDigits(2);
            // create thread to load person
            Thread threadLoadPerson = new Thread(() -> loadPerson());
            threadLoadPerson.start();
            // create thread to load movie
            Thread threadLoadMovie = new Thread(() -> loadMovie());
            threadLoadMovie.start();
            // create thread to load table direct and write
            Thread threadLoadDircWrit = new Thread(() -> loadDirectAndWrite());
            threadLoadDircWrit.start();
            // create thread to load table act and produce
            Thread threadLoadActProd =
                new Thread(() -> loadActAndProduce());
            threadLoadActProd.start();
            // create table to load ratings
            Thread threadLoadRatings = new Thread(() -> loadRatings());
            threadLoadRatings.start();
            // wait for all threads finish to report time
            threadLoadDircWrit.join();
            threadLoadRatings.join();
            threadLoadPerson.join();
            threadLoadMovie.join();
            threadLoadActProd.join();
            long end = System.currentTimeMillis();
            System.out.println("Finish loading all data within " +
                df.format((end - start) / 1000.0 / 60) + " minutes.");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Main function to load rating data from file to database.
     */
    private static void loadRatings() {
        Connection con = null;
        try {
            Class.forName(driver);
            con = DriverManager.getConnection(url, user, password);
            con.setAutoCommit(false);
            Statement statement = con.createStatement();
            String filename = filepath.get("title_ratings");
            BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(filename)));
            reader.readLine();
            String line;
            long start = System.currentTimeMillis();
            StringBuilder sbuilder = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                sbuilder.append("insert into rating values(");
                String[] values = line.trim().replace("'", "''").split("\t");
                // mconst
                sbuilder.append(values[0].substring(2)).append(",");
                // avgRating
                sbuilder.append(values[1].equals("\\N") ? "null" : values[1])
                    .append(",");
                // numVotes
                sbuilder.append(values[2].equals("\\N") ? "null" : values[2])
                    .append(");");
                statement.execute(sbuilder.toString());
                sbuilder.setLength(0);
            }
            con.commit();
            long end = System.currentTimeMillis();
            System.out.println("Thread loads ratings within " +
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
     * Main function to load data from file to table act and produce
     */
    private static void loadActAndProduce() {
        Connection con = null;
        try {
            Class.forName(driver);
            con = DriverManager.getConnection(url, user, password);
            con.setAutoCommit(false);
            Statement statement = con.createStatement();
            BufferedReader reader = new BufferedReader(new InputStreamReader(
                new FileInputStream(filepath.get("title_principals"))));
            reader.readLine();
            String line;
            long start = System.currentTimeMillis();
            while ((line = reader.readLine()) != null) {
                String[] values = line.trim().replace("'", "''").split("\t");
                StringBuilder sbuilder = new StringBuilder();
                if (values[4].equals("actor") ||
                    values[4].equals("producer")) {
                    // build insert statement for act and produce
                    sbuilder.append("insert into ")
                        .append(values[4].equals("actor") ? "act" : "produce")
                        .append(" values(");
                    sbuilder.append(values[0].substring(2)).append(",");
                    sbuilder.append(values[2].substring(2)).append(");");
                    statement.execute(sbuilder.toString());
                    sbuilder.setLength(0);
                }
            }
            con.commit();
            long end = System.currentTimeMillis();
            System.out.println("Thread loads act and produce within " +
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
     * Main function to load data from file to table direct and write
     */
    private static void loadDirectAndWrite() {
        Connection con = null;
        try {
            Class.forName(driver);
            con = DriverManager.getConnection(url, user, password);
            con.setAutoCommit(false);
            Statement statement = con.createStatement();
            String filename = filepath.get("title_crew");
            BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(filename)));
            reader.readLine();
            String line;
            long start = System.currentTimeMillis();
            while ((line = reader.readLine()) != null) {
                String[] values = line.trim().replace("'", "''").split("\t");
                // build statement for direct relationship
                if (!values[1].equals("\\N")) {
                    for (String tt : values[1].split(",")) {
                        StringBuilder directStat = new StringBuilder();
                        directStat.append("insert into direct values(");
                        directStat.append(values[0].substring(2)).append(",");
                        directStat.append(tt.substring(2)).append(");");
                        statement.execute(directStat.toString());
                        directStat.setLength(0);
                    }
                }
                // build statement for write relationship
                if (!values[2].equals("\\N")) {
                    StringBuilder writeStat = new StringBuilder();
                    for (String tt : values[2].split(",")) {
                        writeStat.append("insert into write values(");
                        writeStat.append(values[0].substring(2)).append(",");
                        writeStat.append(tt.substring(2)).append(");");
                        statement.execute(writeStat.toString());
                        writeStat.setLength(0);
                    }
                }
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
     * Main function to load information from file and insert some into table movie.
     */
    private static void loadMovie() {
        Connection con = null;
        try {
            Class.forName(driver);
            con = DriverManager.getConnection(url, user, password);
            con.setAutoCommit(false);
            Statement statement = con.createStatement();
            BufferedReader titleReader = new BufferedReader(
                new InputStreamReader(
                    new FileInputStream(filepath.get("title_basics"))));
            titleReader.readLine();
            String titleLine;
            StringBuilder sbuilder = new StringBuilder();
            long start = System.currentTimeMillis();
            while ((titleLine = titleReader.readLine()) != null) {
                sbuilder.append("insert into movie values(");
                String[] ttValues =
                    titleLine.trim().replace("'", "''").split("\t");
                // mconst
                sbuilder.append(ttValues[0].substring(2)).append(",'");
                // title
                sbuilder.append(ttValues[3]).append("',");
                // releaseTime/ startYear
                sbuilder
                    .append(ttValues[5].equals("\\N") ? "null" : ttValues[5])
                    .append(",");
                // runtimeMinutes
                sbuilder
                    .append(ttValues[7].equals("\\N") ? "null" : ttValues[7])
                    .append(",'{");
                // genres
                if (!ttValues[8].equals("\\N")) {
                    for (String tt : ttValues[8].split(",")) {
                        sbuilder.append(tt).append(",");
                    }
                    sbuilder.setLength(sbuilder.length() - 1);
                }
                sbuilder.append("}');");
                statement.execute(sbuilder.toString());
                sbuilder.setLength(0);
            }
            con.commit();
            long end = System.currentTimeMillis();
            System.out.println("Thread loads movie within " +
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
     * Main function to read file name.basics.tsv and insert some information
     * from file to table in database.
     */
    private static void loadPerson() {
        Connection con = null;
        try {
            Class.forName(driver);
            con = DriverManager.getConnection(url, user, password);
            con.setAutoCommit(false);
            Statement statement = con.createStatement();
            String filename = filepath.get("name_basics");
            BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(filename)));
            // skip the first line
            reader.readLine();
            String line;
            StringBuilder sbuilder = new StringBuilder();
            long start = System.currentTimeMillis();
            while ((line = reader.readLine()) != null) {
                String[] values = line.trim().replace("'", "''").split("\t");
                sbuilder.append("insert into person values(");
                // pconst id
                sbuilder.append(values[0].substring(2)).append(",'");
                // name
                sbuilder.append(values[1].equals("\\N") ? "null" : values[1])
                    .append("',");
                // brith year
                sbuilder.append(values[2].equals("\\N") ? "null" : values[2])
                    .append(",");
                // death year
                sbuilder.append(values[3].equals("\\N") ? "null" : values[3])
                    .append(");");
                statement.execute(sbuilder.toString());
                sbuilder.setLength(0);
            }
            con.commit();
            long end = System.currentTimeMillis();
            System.out.println("Thread loads person within " +
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
}
