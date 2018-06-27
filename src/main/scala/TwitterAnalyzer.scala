import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming._


object TwitterAnalyzer {
  def main(args: Array[String]) :Unit = {
    // Parse input argument for accessing twitter streaming api
    if (args.length < 4) {
      System.err.println("Usage: Demo <consumer key> <consumer secret> " +"<access token> <access token secret> [<filters>]")
      System.exit(1)
    }

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length -4)

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val conf = new SparkConf().setAppName("Demo").setMaster("local[*]").set("spark.driver.memory", "12g").set("spark.executor.memory", "4g")
    val ssc = new StreamingContext(conf, Seconds(10))


    // creating twitter input stream
    // type DStream
    val tweets = TwitterUtils.createStream(ssc, None, filters)


    // Apply the sentiment analysis using detectSentiment function in the SentimentAnalysisUtils
    // for some reason ABSOLUTELY HAVE to do this here
    // otherwise doesn't recognize tweets
    // filter out the non-English tweets and tweets with no hashtags
    tweets.foreachRDD{(rdd) =>
      println(rdd.count())

      rdd.map(t => {

        // don't need retweet and location, only keeping user to show
        // tweets are not duplicate prints, but copy-paste between users
        Map(
          "user"-> t.getUser.getScreenName,
          //"location" -> Option(t.getGeoLocation).map(geo => { s"${geo.getLatitude},${geo.getLongitude}" }),
          "text" -> t.getText,
          "hashtags" -> t.getHashtagEntities.map(_.getText),
          //"retweet" -> t.getRetweetCount,
          "language" -> t.getLang,
          "sentiment" -> SentimentAnalysisUtils.detectSentiment(t.getText).toString)
      }).filter(v => {
        val tweetArray = v("hashtags").asInstanceOf[Array[String]]
        v("language").equals("en") && tweetArray.length > 0
      })

      /*
      val topics = values.flatMap(v => {
        v("hashtags").asInstanceOf[Array[String]]
      }).map(h => {
        (h, 1)
      }).reduceByKey((x,y) => x+y)


      topics.foreach(t =>  println(t._1 + "," + t._2))
      */

      // this prints all the hashtagsval hashtags = v("langague")
      /*values.foreach(bla => {
        println(bla.keys)
        println(bla.values)
        println(bla("language"))
        val hashtags = bla("hashtags")

        if (hashtags != null) {


          println("PRINTING HASHTAGS")
         // println(hashtags.getClass)
          var hashtagArr = hashtags.asInstanceOf[Array[Any]]
          hashtagArr.foreach(hash => {

            println(hash.getClass)
            if (hash.isInstanceOf[String]) {
              println(hash.toString)

            }
            else {
              println("nada")
            }


          })
        }
      });*/
    }

    // this is our updateState function
    // key: topic, value: count in the current window
    // state: old count
    // output: (topic, newCount - oldCount)
    def updateTopicCounts(key: String,
                          value: Option[Int],
                          state: State[Int]): (String, Int) =
    {
      val existingCount: Int =
        state
          .getOption()
          .getOrElse(0)

      val newCount = value.getOrElse(0)

      state.update(newCount)
      (key, newCount - existingCount)
    }

    // define StateSpec for mapWithState
    val stateSpec = StateSpec.function(updateTopicCounts _)

    // create our window
    // transform tweets into mapped tweets
    // filter out non-English and 0-hashtag tweets
    // windowDuration: 45s, slideDuration 15s
    val window = tweets.transform(rdd => {
      rdd.map(t => {
        Map(
          "user"-> t.getUser.getScreenName,
          //"location" -> Option(t.getGeoLocation).map(geo => { s"${geo.getLatitude},${geo.getLongitude}" }),
          "text" -> t.getText,
          "hashtags" -> t.getHashtagEntities.map(_.getText),
          //"retweet" -> t.getRetweetCount,
          "language" -> t.getLang,
          "sentiment" -> SentimentAnalysisUtils.detectSentiment(t.getText).toString)
      })
    }).filter(v => {
      val tweetArray = v("hashtags").asInstanceOf[Array[String]]
      v("language").equals("en") && tweetArray.length > 0
    }).window(Seconds(60), Seconds(60))


    // now we calculate the emerging topics
    val topics = window.flatMap(t => t("hashtags").asInstanceOf[Array[String]])
        .map(h => (h, 1))
        .reduceByKey(_+_)
        .mapWithState(stateSpec)
        .map{case (topic, count) => (count, topic)}
        .transform(_.sortByKey(false))

    // extract the emerging topic from the topics DStream
    var emergingTopic = ""
    topics.foreachRDD(rdd => {
      val top = rdd.take(1)
      top.foreach{case (count, topic) => emergingTopic = topic}
    })

    // back to our window of tweets
    // print all the tweets in our window with the emerging topic
    // we coalesce before saving to prevent empty files and to bring
    // all the RDDs back together
    window.foreachRDD(rdd => {
      println("Emerging topic: %s".format(emergingTopic))
        rdd.filter(t => {
          val hashtagList = t("hashtags").asInstanceOf[Array[String]]
          hashtagList.contains(emergingTopic)
        }).coalesce(1).saveAsTextFile("./output_"+emergingTopic)
    })

    // gotta define a checkpoint directory for mapWithState
    ssc.checkpoint("./checkpoint")
    // Start streaming
    ssc.start()

    // Wait for Termination
    ssc.awaitTermination()
  }
}
