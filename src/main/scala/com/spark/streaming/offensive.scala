package com.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

object offensive {

  //ArrayBuffer to store list of offensive words in memory
  val wordList: ArrayBuffer[String] = ArrayBuffer.empty[String]

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: OffensiveWordCount <hostname> <port>")
      System.exit(1)
    }
    //StreamingExamples.setStreamingLogLevels()
    // Create the context with a 60 second batch size
    val sparkConf = new SparkConf().setAppName("OffensiveWordCount").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(60))
    val words = Array("shit","damn","idiot","stupid","dash")
    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    val wordcounts = lines.flatMap(line => line.split(" ").filter(w=> words.contains(w.toLowerCase)).map(word =>(word,1))).reduceByKey(_ + _)
    wordcounts.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
