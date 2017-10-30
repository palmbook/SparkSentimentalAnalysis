// This package name should be replaced
package com.palmbook.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._

import scala.io.Source

// You need to import external JARs for twitter streaming
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._

// On the first version, CoreNLP was used to do sentiment analysis
// However, the result was not good, so I changed to a simpler approach

/** Listens to a stream of Tweets and keeps track of the most popular
 *  hashtags over a 5 minute window.
 */
object stockSentimentTwitter {
  
  // Directory to cache your twitter checkpoint
  // Please update
  val checkpointDir = "/home/charles/twitter"
  
  // List of keywords that we want to watch about equity markets
  val equityKeywordList : List[String] = List("nyse", "market", "trading",
      "stock", "finance", "money", "#otc", "nasdaq", "ftse", "#cac", "dax", 
      "nikkei", "hangseng", "asx200", "setindex", "set50", "#future", "investing")
      
  val metalKeywordList : List[String] = List("preciousmetal", "preciousmetals", "gold",
      "silver", "bullion", "commodities", "metals", "coins")
  
  val cryptoKeywordList : List[String] = List("bitcoin", "blockchain", "ethereum",
      "crypto", "cryptocurrency", "btc", "fintech", "ico", "eth", "tokensale")
      
  
  def loadAfinn() : Map[String, Int] = {
    
    var afinnMap:Map[String, Int] = Map()
    
    val AFINN = Source.fromFile("../AFINN.txt").getLines()
    
    for (line <- AFINN){
      var fields = line.split('\t')
      afinnMap += (fields(0).toString -> fields(1).toInt)
    }
    
     return afinnMap
  }
  
  def mapSentiment(word :String, afinnMap: Map[String, Int]) : Option[(String, Int)] = {
    if (afinnMap.contains(word)) 
    { 
      return Some(word, afinnMap(word)) 
      } 
    else 
    { 
      return None 
      }
  }
  
  def interpretScore(kv : (String, Float)) : (String, String, Float) = {
    if (kv._2 > 2.0) 
    {
      return (kv._1, "VERY POSITIVE", kv._2)
    }
    if (kv._2 > 0.5)
    {
      return (kv._1, "POSITIVE", kv._2)
    }
    if (kv._2 < -2.0)
    {
      return (kv._1, "VERY NEGATIVE", kv._2)
    }
    if (kv._2 < -0.5)
    {
      return (kv._1, "NEGATIVE", kv._2)
    }
    
    return (kv._1, "NEUTRAL", kv._2)
  }
  
    /** Makes sure only ERROR messages get logged to avoid log spam. */
  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}   
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)   
  }
 
  
  /** Configures Twitter service credentials using twiter.txt in the main workspace directory */
  def setupTwitter() = {
    import scala.io.Source
    
    for (line <- Source.fromFile("../twitter.txt").getLines) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
  }
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()
    
    val ssc = new StreamingContext("local[*]", "Market Sentiment", Seconds(15))
    
    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None)
    
    // Now extract the text of each status update into DStreams using map()
    val statuses = tweets.map(status => status.getText()).cache()
    
    val afinnMap = loadAfinn()
    
    /********************************************************************/
    /** In this section, we compute sentiments and hashtags for equity **/
    /********************************************************************/
    
    // We want only tweets which contain our keywords
    val relevantTweetsEquity = statuses.filter(word => equityKeywordList.exists(word.toLowerCase.contains))
    
    // For each tweet, gauge the sentiment
   
    
    // For each survivor, extract words
    val tweetwordsEquity = relevantTweetsEquity.flatMap(tweetText => tweetText.split(" "))
    
    // For each word, map sentiment
    val wordSentimentEquity = tweetwordsEquity.flatMap(word => mapSentiment(word, afinnMap))
    
    // We will then count the occurrence of each non-neutral word
    val wordSentiKVEquity = wordSentimentEquity.map(kv => (kv, 1))
    
    // Now count them up over a 5 minute window sliding every one second
    val totalWordSentiEquity = wordSentiKVEquity.reduceByKeyAndWindow( (x,y) => x + y, (x,y) => x - y, Seconds(300), Seconds(15)).cache()
    
    // Total number of words in concern
    val amountWordsEquity = totalWordSentiEquity.map(kv => kv._2).reduce(_+_).map(kv => ("equity", kv))
    
    // Compute the sentiment score
    val finalSentiScoreEquity = totalWordSentiEquity.map(kv => (kv._1._2 * kv._2)).reduce(_+_).map(kv => ("equity", kv))

    // Calculate score average
    val equityAvgScore = finalSentiScoreEquity.join(amountWordsEquity).map(kv => (kv._1, kv._2._1.toFloat/kv._2._2.toFloat))
    
    val interpretedEquityScore = equityAvgScore.map(interpretScore)
    
    /********************************************************************/
    /** In this section, we compute sentiments and hashtags for gold   **/
    /********************************************************************/
    
    // We want only tweets which contain our keywords
    val relevantTweetsGold = statuses.filter(word => metalKeywordList.exists(word.toLowerCase.contains))
    
    // For each tweet, gauge the sentiment
   
    
    // For each survivor, extract words
    val tweetwordsGold = relevantTweetsGold.flatMap(tweetText => tweetText.split(" "))
    
    // For each word, map sentiment
    val wordSentimentGold = tweetwordsGold.flatMap(word => mapSentiment(word, afinnMap))
    
    // We will then count the occurrence of each non-neutral word
    val wordSentiKVGold = wordSentimentGold.map(kv => (kv, 1))
    
    // Now count them up over a 5 minute window sliding every one second
    val totalWordSentiGold = wordSentiKVGold.reduceByKeyAndWindow( (x,y) => x + y, (x,y) => x - y, Seconds(300), Seconds(15)).cache()
    
    // Total number of words in concern
    val amountWordsGold = totalWordSentiGold.map(kv => kv._2).reduce(_+_).map(kv => ("precious-metals", kv))
    
    // Compute the sentiment score
    val finalSentiScoreGold = totalWordSentiGold.map(kv => (kv._1._2 * kv._2)).reduce(_+_).map(kv => ("precious-metals", kv))

    // Calculate score average
    val GoldAvgScore = finalSentiScoreGold.join(amountWordsGold).map(kv => (kv._1, kv._2._1.toFloat/kv._2._2.toFloat))
    
    val interpretedGoldScore = GoldAvgScore.map(interpretScore)
    
    /********************************************************************/
    /** In this section, we compute sentiments and hashtags for CryptoCur**/
    /********************************************************************/
    
    // We want only tweets which contain our keywords
    val relevantTweetsCrypto = statuses.filter(word => cryptoKeywordList.exists(word.toLowerCase.contains))
    
    // For each tweet, gauge the sentiment
   
    
    // For each survivor, extract words
    val tweetwordsCrypto = relevantTweetsCrypto.flatMap(tweetText => tweetText.split(" "))
    
    // For each word, map sentiment
    val wordSentimentCrypto = tweetwordsCrypto.flatMap(word => mapSentiment(word, afinnMap))
    
    // We will then count the occurrence of each non-neutral word
    val wordSentiKVCrypto = wordSentimentCrypto.map(kv => (kv, 1))
    
    // Now count them up over a 5 minute window sliding every one second
    val totalWordSentiCrypto = wordSentiKVCrypto.reduceByKeyAndWindow( (x,y) => x + y, (x,y) => x - y, Seconds(300), Seconds(15)).cache()
    
    // Total number of words in concern
    val amountWordsCrypto = totalWordSentiCrypto.map(kv => kv._2).reduce(_+_).map(kv => ("crypto-currency", kv))
    
    // Compute the sentiment score
    val finalSentiScoreCrypto = totalWordSentiCrypto.map(kv => (kv._1._2 * kv._2)).reduce(_+_).map(kv => ("crypto-currency", kv))

    // Calculate score average
    val CryptoAvgScore = finalSentiScoreCrypto.join(amountWordsCrypto).map(kv => (kv._1, kv._2._1.toFloat/kv._2._2.toFloat))
    
    val interpretedCryptoScore = CryptoAvgScore.map(interpretScore)
    
    /********************************************************************/
    /** Print out all results                                          **/
    /********************************************************************/
   
    interpretedEquityScore.print()
    interpretedGoldScore.print()
    interpretedCryptoScore.print()

    
    ssc.checkpoint(checkpointDir)
    ssc.start()
    ssc.awaitTermination()
  }  
}
