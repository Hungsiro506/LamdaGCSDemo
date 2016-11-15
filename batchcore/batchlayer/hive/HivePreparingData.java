package com.gcs.batchcore.batchlayer.hive;


import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.gcs.batchcore.infrastructure.Constants;
/**
 * @author hungdv
 * @since 7-8-2015
 */
import com.gcs.batchcore.utils.HiveConfiguration;

public class HivePreparingData {

  static Logger log = Logger.getLogger(HivePreparingData.class.getName());
  static HiveService hsi = new HiveService(HiveConfiguration.HOST, HiveConfiguration.PORT,
      HiveConfiguration.DATABASE_NAME2, HiveConfiguration.USER_NAME, HiveConfiguration.PASSWORD);

  private static String dbname; // HiveConfiguration.DATABASE_NAME;
  private static String output;

  public HivePreparingData(String pdbname, String poutput) {

    dbname = pdbname;
    output = poutput;
  }

  /**
   * run just one time
   * 
   * @throws SQLException
   */
  public static void init() throws SQLException {
    hsi.openConnection();

    // Step0:
    initDatabase();
    // Step1:
    initTable();
    // Step2:
    initData();

    hsi.closeConnection();
  }

  private static void initDatabase() throws SQLException {

    String CreateDatabase = "CREATE DATABASE IF NOT EXISTS " + HiveConfiguration.DATABASE_NAME2;
    hsi.executeNonQuery(CreateDatabase);
    System.out.println("init DataBase");
  }

  private static void initTable() throws SQLException {
    /**
     * All tables except final table contains two columns : the first column is project id used to
     * join them together. the second column has the same name with the table. this column store a
     * part of data from final column In the end, we will join all single column to insert data into
     * final table.
     */
    // Hardcode everywhere !!!

    final String teamsize = "teamsize";
    final String projectduration = "projectduration";
    /*
     * final String projectsize = "projectsize"; final String avgyearsexperience =
     * "avgyearsexperience"; final String avgperformance = "avgperformance"; final String bugs =
     * "bugs"; final String bugeffort = "bugeffort"; final String totaleffort = "totaleffort"; final
     * String oteffort = "oteffort";
     */



    if (hsi.getConnection() != null) {

      // 1.1 Team size
      // hsi.executeNonQuery("DROP TABLE IF  EXISTS teamsize ");
      hsi.deleteTable(teamsize);
      hsi.executeNonQuery("CREATE TABLE IF NOT EXISTS teamsize (projectid INT,teamsize INT)");

      // 1.2.1 Project duration temp (contain NULL)
      hsi.executeNonQuery("DROP TABLE IF  EXISTS projectdurationtemp ");
      hsi.executeNonQuery("CREATE TABLE IF NOT EXISTS projectdurationtemp (projectid INT,projectduration INT)");

      // 1.2.2 Project duration
      hsi.deleteTable(projectduration);
      // hsi.executeNonQuery("DROP TABLE IF  EXISTS projectduration ");
      hsi.executeNonQuery("CREATE TABLE IF NOT EXISTS projectduration (projectid INT,projectduration INT)");

      // 1 Project size = team size * project duration
      hsi.executeNonQuery("DROP TABLE IF  EXISTS projectsize ");
      hsi.executeNonQuery("CREATE TABLE IF NOT EXISTS projectsize (projectid INT,projectsize INT)");

      // 2 AVG years of experience of team's member: [ Cancel this factor due to lacking of info]
      hsi.executeNonQuery("DROP TABLE IF  EXISTS avgyearsexperience ");
      hsi.executeNonQuery("CREATE TABLE IF NOT EXISTS avgyearsexperience (projectid INT,avgyearsexperience FLOAT)");

      // 3 AVG performance rate [Cancel this factor due to lacking of info]
      hsi.executeNonQuery("DROP TABLE IF  EXISTS avgperformance ");
      hsi.executeNonQuery("CREATE TABLE IF NOT EXISTS avgperformance (projectid INT,avgperformance FLOAT)");

      // 4 Current bugs :
      hsi.executeNonQuery("DROP TABLE IF  EXISTS bugs ");
      hsi.executeNonQuery("CREATE TABLE IF NOT EXISTS bugs (projectid INT,bugs INT)");

      // 5 Total current time to fixing bugs :
      hsi.executeNonQuery("DROP TABLE IF  EXISTS bugeffort ");
      hsi.executeNonQuery("CREATE TABLE IF NOT EXISTS bugeffort (projectid INT,bugeffort INT)");

      // 6 Total current effort :
      hsi.executeNonQuery("DROP TABLE IF  EXISTS totaleffort ");
      hsi.executeNonQuery("CREATE TABLE IF NOT EXISTS totaleffort (projectid INT,totaleffort INT)");

      // 7.1 OT time > 8 hour/day (time_worked > 36000) : Ot1
      hsi.executeNonQuery("DROP TABLE IF  EXISTS ot1 ");
      hsi.executeNonQuery("CREATE TABLE IF NOT EXISTS ot1 (projectid INT,ot1 INT)");
      // 7.2 OT time on Saturday and Sunday: Ot2
      hsi.executeNonQuery("DROP TABLE IF  EXISTS ot2 ");
      hsi.executeNonQuery("CREATE TABLE IF NOT EXISTS ot2 (projectid INT,ot2 INT)");

      // 7 Total current OT effort = Ot1 + Ot2:
      hsi.executeNonQuery("DROP TABLE IF  EXISTS oteffort ");
      hsi.executeNonQuery("CREATE TABLE IF NOT EXISTS oteffort (projectid INT,oteffort INT)");

      // 8 Current Success rate :
      hsi.executeNonQuery("DROP TABLE IF  EXISTS csr ");
      hsi.executeNonQuery("CREATE TABLE IF NOT EXISTS csr (projectid INT,csr FLOAT)");

      // 9 Final prepare out put -> input for spark
      hsi.executeNonQuery("DROP TABLE IF  EXISTS input ");
      hsi.executeNonQuery("CREATE TABLE IF NOT EXISTS input (projectid INT,projectsize INT,"
          + "bugs INT," + "bugeffort INT,"
          + "oteffort INT,totaleffort INT,teamsize INT,crs FLOAT) "
          + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ");
      /*
       * + "LOCATION 'user/hive/warehouse/hungdv/input' " + "STORED AS TEXTFILE" );
       */
      // close connection
      log.info("Init table successfully");
      System.out.println("init table");
    }
  }

  // run just one time from beginning of time.
  public static void initData() throws SQLException {
    if (hsi.getConnection() != null) {
      String space = " ";
      System.out.println("init data startting");
      // project_duration \\
      String project_duration_temp =
          "INSERT INTO hungdv.projectdurationtemp "
              + space
              + "select jiraissue.project, DATEDIFF(Max(projectversion.releasedate), MIN(jiraissue.created)) "
              + space + "from " + dbname + ".jiraissue full join " + dbname
              + ".projectversion on jiraissue.project = projectversion.project " + space
              + "group by jiraissue.project";
      hsi.executeNonQuery(project_duration_temp);
      System.out.println("2");
      String project_duration =
          "INSERT INTO hungdv.projectduration "
              + space
              + "select projectdurationtemp.projectid, "
              + space
              + "CASE "
              + space
              + "WHEN projectdurationtemp.projectduration is NULL then 0"
              + space
              + "WHEN projectdurationtemp.projectduration is not NULL then projectdurationtemp.projectduration END"
              + space + "from hungdv.projectdurationtemp";
      hsi.executeNonQuery(project_duration);
      log.info("Init table : project_duration ");

      // team_size \\
      String team_size =
          "INSERT INTO hungdv.teamsize " + space + "select project, count(distinct assignee) "
              + space + "from " + dbname + ".jiraissue " + space + "group by project ";
      hsi.executeNonQuery(team_size);
      log.info("Init table : teamsize ");
      System.out.println("init teamsize");
      // project_size \\
      String project_size =
          "INSERT INTO hungdv.projectsize "
              + space
              + "select projectduration.projectid, (projectduration.projectduration*teamsize.teamsize ) "
              + space
              + " from hungdv.projectduration full join hungdv.teamsize on projectduration.projectid = teamsize.projectid"
              + space + "where projectduration.projectduration >0 ";
      hsi.executeNonQuery(project_size);
      log.info("Init table : project size ");
      System.out.println("init projectsize");

      // current bugs \\:
      String current_bugs =
          "INSERT INTO hungdv.bugs " + space + "select jiraissue.project,Count(jiraissue.id)  "
              + space + "from " + dbname + ".jiraissue join " + dbname
              + ".issuetype on jiraissue.issuetype = issuetype.id " + space
              + "where jiraissue.issuetype = 1 " + space + "group by jiraissue.project";
      hsi.executeNonQuery(current_bugs);
      log.info("Init table : bugs ");
      System.out.println("init bugs");

      // total time to fixing bug - bug effort: \\
      String bug_effort =
          "INSERT INTO hungdv.bugeffort " + space
              + "select jiraissue.project, SUM(timespent) from " + dbname + ".jiraissue " + space
              + "where issuetype = 1 and timespent is not null " + space
              + "group by jiraissue.project ";
      hsi.executeNonQuery(bug_effort);
      log.info("Init table : bug effort ");


      // total effort: \\
      String total_effort =
          "INSERT INTO hungdv.totaleffort " + space
              + "select jiraissue.project, SUM(jiraissue.timespent) as total_effort " + space
              + "from " + dbname + ".jiraissue " + space + "group by jiraissue.project ";
      hsi.executeNonQuery(total_effort);
      log.info("Init table : total effort ");
      System.out.println("init total effort");

      // Ot1 : Working time > 8 hours per day
      String ot1 =
          "INSERT INTO hungdv.ot1 "
              + space
              + " select jiraissue.project, (sum(worklog.timeworked -36000)) as OT1 "
              + space
              + "from "
              + dbname
              + ".jiraissue join "
              + dbname
              + ".worklog on jiraissue.id = worklog.issueid "
              + space
              + "where worklog.timeworked > 36000 and from_unixtime(unix_timestamp(startdate),'u') != 5 and  from_unixtime(unix_timestamp(startdate),'u') != 6 "
              + space + "group by jiraissue.project";
      hsi.executeNonQuery(ot1);
      log.info("Init table : ot1 ");

      // Ot2 : Work on sun and sat.
      String ot2 =
          "INSERT INTO hungdv.ot2 "
              + space
              + " select jiraissue.project, (sum(worklog.timeworked)) as OT2 "
              + space
              + "from "
              + dbname
              + ".jiraissue join "
              + dbname
              + ".worklog on jiraissue.id = worklog.issueid "
              + space
              + "where  from_unixtime(unix_timestamp(startdate),'u') = 5 or from_unixtime(unix_timestamp(startdate),'u') = 6 "
              + space + "group by jiraissue.project ";
      hsi.executeNonQuery(ot2);
      log.info("Init table : ot2 ");


      // total OT effort :
      String ot_effort =
          "INSERT INTO hungdv.oteffort " + space + "select a.projectid, (a.ot1 + b.ot2) " + space
              + "from hungdv.ot1 a join hungdv.ot2 b on a.projectid = b.projectid ";
      hsi.executeNonQuery(ot_effort);
      log.info("Init table : OT effort ");
      System.out.println("init OT effort");

      // final table - input for spark

      // USING AVERAGE VALUE FOR NULL FACTOR //
      // IF WE HAVE LARGE AMOUN OF DATA, SHOUD YOU THIS MENTHOD //
      // ///////////////////////////////// //////////////////////

      /*
       * String final_table = "INSERT INTO hungdv.input " + space + "select a5.projectid," + space +
       * "COALESCE(a5.projectsize," + getAvg("projectsize") + ")," + space + "COALESCE(a5.bugs," +
       * getAvg("bugs") + ")," + space + "COALESCE(a5.bugeffort," + getAvg("bugeffort") + ")," +
       * space + "COALESCE(a5.oteffort," + getAvg("oteffort") + ")," + space +
       * "COALESCE(a5.totaleffort," + getAvg("totaleffort") + ")," + space + "COALESCE(a5.teamsize,"
       * + getAvg("teamsize") + ")," + space + "COALESCE((1/(COALESCE(a5.bugeffort," +
       * getAvg("bugeffort") + ")/ COALESCE(a5.totaleffort," + getAvg("totaleffort") +
       * ")+ COALESCE(a5.oteffort,0)/ COALESCE(a5.totaleffort," + getAvg("totaleffort") +
       * ") + COALESCE(a5.bugs," + getAvg("bugs") + ")/ COALESCE(a5.projectsize," +
       * getAvg("projectsize") + ") )),1)" + space +
       * "from (select a4.projectid,a4.teamsize,a4.bugs,a4.bugeffort,a4.totaleffort,a4.projectsize,oteffort.oteffort"
       * + space +
       * "from (select a3.projectid,a3.teamsize,a3.bugs,a3.bugeffort,a3.totaleffort,projectsize.projectsize "
       * + space +
       * "from (select a2.projectid,a2.teamsize,a2.bugs,a2.bugeffort,totaleffort.totaleffort" +
       * space + "from (select a.projectid,a.teamsize,a.bugs, bugeffort.bugeffort " + space +
       * "from (select teamsize.projectid,teamsize.teamsize, bugs.bugs from hungdv.teamsize full outer join  hungdv.bugs on (teamsize.projectid = bugs.projectid )) a"
       * + space + "full outer join hungdv.bugeffort on (a.projectid = bugeffort.projectid )) a2" +
       * space + "full outer join hungdv.totaleffort on (a2.projectid = totaleffort.projectid )) a3"
       * + space +
       * "full outer join hungdv.projectsize on (a3.projectid = projectsize.projectid)) a4" + space
       * + "full outer join hungdv.oteffort on (a4.projectid = oteffort.projectid)) a5" + space;
       */

      // USING DEFAULT VALUE FOR NULL FACTOR //
      // //
      // DEFAULT VALUE = 1 TO AVOID NOSIE DATA //

      String final_table =
          "INSERT INTO hungdv.input "
              + space
              + "select a5.projectid,"
              + space
              + "COALESCE(a5.projectsize,"
              + Constants.DEFAULT_VALUE
              + "),"
              + space
              + "COALESCE(a5.bugs,"
              + Constants.DEFAULT_VALUE
              + "),"
              + space
              + "COALESCE(a5.bugeffort,"
              + Constants.DEFAULT_VALUE
              + "),"
              + space
              + "COALESCE(a5.oteffort,"
              + Constants.DEFAULT_VALUE
              + "),"
              + space
              + "COALESCE(a5.totaleffort,"
              + Constants.DEFAULT_VALUE
              + "),"
              + space
              + "COALESCE(a5.teamsize,"
              + Constants.DEFAULT_VALUE
              + "),"
              + space
              + "COALESCE((1/(COALESCE(a5.bugeffort,"
              + Constants.DEFAULT_VALUE
              + ")/ COALESCE(a5.totaleffort,"
              + Constants.DEFAULT_VALUE
              + ")+ COALESCE(a5.oteffort,0)/ COALESCE(a5.totaleffort,"
              + Constants.DEFAULT_VALUE
              + ") + COALESCE(a5.bugs,"
              + Constants.DEFAULT_VALUE
              + ")/ COALESCE(a5.projectsize,"
              + Constants.DEFAULT_VALUE
              + ") )),"
              + Constants.DEFAULT_VALUE
              + ")"
              + space
              + "from (select a4.projectid,a4.teamsize,a4.bugs,a4.bugeffort,a4.totaleffort,a4.projectsize,oteffort.oteffort"
              + space
              + "from (select a3.projectid,a3.teamsize,a3.bugs,a3.bugeffort,a3.totaleffort,projectsize.projectsize "
              + space
              + "from (select a2.projectid,a2.teamsize,a2.bugs,a2.bugeffort,totaleffort.totaleffort"
              + space
              + "from (select a.projectid,a.teamsize,a.bugs, bugeffort.bugeffort "
              + space
              + "from (select teamsize.projectid,teamsize.teamsize, bugs.bugs from hungdv.teamsize full outer join  hungdv.bugs on (teamsize.projectid = bugs.projectid )) a"
              + space
              + "full outer join hungdv.bugeffort on (a.projectid = bugeffort.projectid )) a2"
              + space
              + "full outer join hungdv.totaleffort on (a2.projectid = totaleffort.projectid )) a3"
              + space
              + "full outer join hungdv.projectsize on (a3.projectid = projectsize.projectid)) a4"
              + space
              + "full outer join hungdv.oteffort on (a4.projectid = oteffort.projectid)) a5"
              + space;
      hsi.executeNonQuery(final_table);

      log.info("Init table : final.");
      log.info("Init data successfully.");
      System.out.println("Init data successfully.");
    }
  }


  // [ DON'T NEED TO IMPLEMENT NOW - 31/7]
  // re-run every day, month or when a new project created
  /**
   * Insert data has date after day yyyy/MM//dd Parameter date : date-time to import data
   * 
   * @throws SQLException
   */
  public static void insertData(String date) throws SQLException {}



  // re-run every day or every month to Update Non-release Project \\
  /**
   * automatic update changed data.
   */
  public static void updateData() {
    // do nothing
  }

  // Save to HDFS \\
  public static void saveToHDFS() throws SQLException {
    String space = " ";

    String savetoHDFS =
        "INSERT OVERWRITE DIRECTORY '" + output + "'" + space + "ROW FORMAT DELIMITED " + space
            + "FIELDS TERMINATED BY ',' " + space + "STORED AS TEXTFILE " + space
            + "SELECT * FROM input";
    hsi.openConnection();
    hsi.executeNonQuery(savetoHDFS);
    hsi.closeConnection();
  }

  public static void preCleanDataSource() throws SQLException {
    hsi.openConnection();
    // do nothing, data source don't need to be cleaned
    hsi.closeConnection();
  }

  public static void cleanData() throws SQLException {
    hsi.openConnection();
    // do n
    hsi.closeConnection();
  }

  public static void splitData() throws SQLException {
    hsi.openConnection();
    // do nothing
    hsi.closeConnection();
  }

  // get average value. \\
  public static String getAvg(String table) throws SQLException {

    String sql = "select avg(" + table + ") from " + table + " where " + table + " is not null";
    ResultSet rs = hsi.executeQuery(sql);
    if (rs.next()) {
      return Integer.toString(Math.round(rs.getFloat(1)));
    }
    return Constants.DEFAULT_VALUE;

  }
}
