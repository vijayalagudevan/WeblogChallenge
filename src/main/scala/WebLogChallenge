/*
 * This is a sample scala script to sessionize a web log. Update the getSourceFile method to change the location. 
 * 
 * Copy paste the below code to a spark REPL and execute the last command
 */

package com.paytm.webLogChallenge

import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext

/*
 * create case class with property timestamp, clientip, request
 */
case class AccessLog(timestamp: String, clientIp: String, request: String)

object WebLogChallenge {

  def main(args: Array[String]) {

    //get time window as input. If no input, 15 minutes is default
    val timeWindow = if (args.length > 0) args(0).toInt else 15
    /*
     * set configuration
     */
    setConfiguration()

    //val sc = getSparkContext()
    //val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    /*
     * Read the zipped log file, 
     * split the line by space 
     * extract timestamp, client ip (by exculding port), request url (excluding the query string)
     * transform the rdd to dataframe
     * register temporary table
     */
    sc.textFile(getSourceFile)
      .map(x => x.split(" "))
      .map(x => AccessLog(x(0), x(2).split(':')(0), x(12).split('?')(0)))
      .toDF
      .registerTempTable("tblAccessLog")

    /*
     * create session partition as 1 | 0 based on the difference of timestamp. The session window time is set to 15 mins by default.
     * register temporary table 		      
     */
    sqlContext
      .sql("""select 
              clientIp, request, timestamp, case when unix_timestamp(timestamp, "yyyy-mm-dd'T'hh:mm:ss.SSSSSS'Z'") - lag(unix_timestamp(timestamp, "yyyy-mm-dd'T'hh:mm:ss.SSSSSS'Z'")) 
              OVER (PARTITION BY clientIp ORDER BY timestamp) >= """ + (timeWindow * 60).toString() + """ then 1 else 0 end as sessionPartition 
              from tblAccessLog""")
      .registerTempTable("tblSessionPartition")

    /*
     * aggregate all pages hits by client ip for the given fixed time window   
     */
    sqlContext
      .sql(""" select 
               clientIp, request, timestamp,                
               sum(sessionPartition) OVER (PARTITION BY clientIp ORDER BY timestamp) AS sessionId 
               from tblSessionPartition""")
      .registerTempTable("tblSession")
    
    println("--------------------------Sessionnized Web Log by Client Ip--------------------------")
    sqlContext
      .sql("select clientIp, request, timestamp, sessionId from tblSession")
      .show(100, false)

    /*
     * Take a time difference between max and min to get the session time 
     */
    sqlContext
      .sql("""select 
              clientIp,                
              sessionId, (max(unix_timestamp(timestamp, "yyyy-mm-dd'T'hh:mm:ss.SSSSSS'Z'")) -  min(unix_timestamp(timestamp, "yyyy-mm-dd'T'hh:mm:ss.SSSSSS'Z'")))/100 as sessionTime 
              from tblSession 
              group by clientIp, sessionId""")
      .registerTempTable("tblSessionTime")

    println("--------------------------Average Session Time by Client Ip--------------------------")  
    /* 
     * calculate the average session time  
     */      
    sqlContext
      .sql("""select clientIp,
               sessionId, 
               avg(sessionTime) as avgSessionTime 
               from tblSessionTime 
               group by clientIp, sessionId
               order by avgSessionTime desc""")
      .show(100, false)

     println("--------------------------Unique URL hit per session by Client Ip--------------------------") 
    /*
     * Determine unique URL visit per session
     */
    sqlContext
      .sql("""select clientIp, sessionId, count(distinct request) urlHits 
               from tblSession 
               group by clientIp, sessionId 
               order by  urlHits desc""")
      .show(100, false)

     println("--------------------------Most engaged Client Ip--------------------------") 
    /*
     *  Get the most engaged users based on the max session time
     */
    sqlContext
      .sql("""select
              clientIp, max(sessionTime) as maxTime
              from tblSessionTime
              group by clientIp
              order by maxTime desc
              """)
      .show(100, false)

    /*
     * drop temp tables  
     */
    sqlContext.dropTempTable("tblAccessLog")
    sqlContext.dropTempTable("tblSessionPartition")
    sqlContext.dropTempTable("tblSessionTime")
    sqlContext.dropTempTable("tblSession")
    
  }

  def getSourceFile(): String = {
    "s3n://xxxxxxxxxxx:xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx@mya-disco-in/Vijay/webLog/2015_07_22_mktplace_shop_web_log_sample.log.gz"
  }

  def setConfiguration(): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
  }

  def getSparkContext(): SparkContext = {
    new SparkContext(getSparkConf().set("spark.logConf", "true"))
  }

  def getSparkConf(): SparkConf = {
    val conf = new SparkConf().setAppName("PayTM Web Log Change").setMaster("local")
    return conf
  }
}

//WebLogChallenge.main(Array("15"))
