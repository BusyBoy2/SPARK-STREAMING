package com.spark.streaming

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}

object SparkStreaming {

  def main(args: Array[String]): Unit = {

    // Removing all INFO logs in consol printing only result sets
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)


    val sparkConf = new SparkConf().setAppName("OffensiveWordCount").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")



    val evenlines = sc.accumulator(0)
    val wordtoNumbers = Map("Hi" -> 1, "this" -> 2, "is" -> 3, "Spark" -> 4, "Session" -> 5, "which" -> 6, "Is" -> 7, "Good" -> 8, "for" -> 9, "Learning" -> 10)
    val wordtoBroadcast = sc.broadcast(wordtoNumbers)

    def linewordNumbers(line: String): Int = {
      var sum:Int = 0
      val words = line.split(" ")
      for (word <- words)
        sum += wordtoBroadcast.value.get(word).getOrElse(0)
      sum
    }


    val ssc = new StreamingContext(sc, Seconds(60))
    val stream = ssc.socketTextStream("192.168.1.6", 5555, StorageLevel.MEMORY_AND_DISK_SER)
    stream.foreachRDD(line => {
      val linestr = line.collect().toList.mkString(" ")
      if (linestr != "") {
        var numTotlal = linewordNumbers(linestr);
        if (numTotlal %2 == 1){
          println(linestr)
        }
        else {
          evenlines += numTotlal
          println(" Sum of lines with even words so far:" +evenlines.value.toInt)
        }
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}