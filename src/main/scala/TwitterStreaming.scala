import java.sql.DriverManager

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.{Logger, Status}

object TwitterStreaming extends App {

  val logger = Logger.getLogger (this.getClass)

  val conf = new SparkConf ().setMaster ("local[*]").setAppName ("Spark Streaming - PopularHashTags")
  val sc = new SparkContext (conf)

  sc.setLogLevel ("WARN")
  val blockSize = 10
  val ssc = new StreamingContext (sc, Seconds (blockSize))

  val stream: DStream[Status] = TwitterUtils.createStream (ssc, None)
  val hashTags = stream.flatMap (status => status.getText.split (" ").filter (_.startsWith ("#")))
  ssc.checkpoint ("_checkpoint")

  val windowSize = 60
  val topCounts60 = hashTags.map ((_, 1)).reduceByKeyAndWindow (_ + _, Seconds (windowSize))
    .map { case (topic, count) => (count, topic) }
    .transform (_.sortByKey (false))
  stream.print ()

  val url = "jdbc:mysql://localhost:3306/TwitterApp"
  val username = "root"
  val password = "knoldus"


  /**
    * Fetching Top 3 tweets count in 10 Seconds Block and 60 Seconds Window
    *
    */
  topCounts60.foreachRDD {
    rdd =>
      rdd.take (3).foreach {
        case (count, hashtag) =>
          Class.forName ("com.mysql.jdbc.Driver")
          val conn = DriverManager.getConnection (url, username, password)
          val del = conn.prepareStatement ("INSERT INTO Twitter (Hashtag,Count) VALUES (?,?)")
          del.setString (1, hashtag)
          del.setInt (2, count)
          del.executeUpdate
          conn.close ()
      }
  }

  ssc.start ()
  ssc.awaitTermination ()

}
