import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf

/**
 * Calculates popular hashtags (topics) over sliding 10 and 60 second windows from a Twitter
 * stream. The stream is instantiated with credentials and optionally filters supplied by the
 * command line arguments.
 *
 * Run this on your local machine as
 *
 * https://themepacific.com/how-to-generate-api-key-consumer-token-access-key-for-twitter-oauth/994/
 *
 */
object TwitterPopularTags {
  def main(args: Array[String]) {

    StreamingExamples.setStreamingLogLevels()

    //val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)

    val consumerKey = "Put you Consumer Key here"
    val consumerSecret = "Put you consumerSecret here"
    val accessToken = "Put you accessToken here"
    val accessTokenSecret = "Put you accessTokenSecret here"

    //val filters = args.takeRight(args.length - 4)

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generat OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf().setAppName("TwitterPopularTags").setMaster("local[10]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val stream = TwitterUtils.createStream(ssc, None, Array("covid-19")) // No Authentication

    //stream.print()

    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    hashTags.print()

    /*The transform function in Spark streaming allows one to use any of Apache Spark's transformations on the underlying RDDs for the stream.
			map is used for an element to element transform, and could be implemented using transform. Essentially,
			map works on the elements of the DStream and transform allows you to work with the RDDs of the DStream.*/

    val topCounts60 = hashTags.map((_, 1))
      .reduceByKeyAndWindow((x, y) => x + y, Seconds(60))
      .map(x => x.swap)
      .transform(_.sortByKey(false))

    topCounts60.print()

    topCounts60.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
    })

    //    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
    //      .map { case (topic, count) => (count, topic) }
    //      .transform(_.sortByKey(false))

    //     Print popular hashtags

    /*topCounts10.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
    })*/

    ssc.start()
    ssc.awaitTermination()
  }
}
