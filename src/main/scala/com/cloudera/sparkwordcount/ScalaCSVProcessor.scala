package com.cloudera.sparkwordcount

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import scala.collection.mutable.ListBuffer


object ScalaCSVProcessor {
  def func1(s: String): TraversableOnce[String] = {
    System.out.println("In func1")
    System.out.println(s)
    var sList = new Array[String](3)
    sList = s.split(" ")
    var str = ""
    for (strCur <- sList) {
      str = str + "_" + strCur
    }
    return List(sList(0),str)
  }

  def func2(s: String): Tuple2[String,Int] = {
    System.out.println("In func2")
    System.out.println(s)

    return Tuple2(s,1)
  }

  def func3(s: String): Tuple2[String,Array[String]] = {
    System.out.println("In func3")
    System.out.println(s)

    var sList = new Array[String](3)
    sList = s.split(" ")
    var str = ""
    for (strCur <- sList) {
      str = str + "_" + strCur
    }

    return Tuple2(sList(0),sList)
  }

  def func4(t1: Array[String], t2 : Array[String]): Array[String] = {
    System.out.println("In func4")

    var sList = t1 ++ t2

    return sList
  }

  def main(args: Array[String]) {
    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))

    // get threshold
    val threshold = args(1).toInt

    // read in text file and split each document into words
    //val tokenized = sc.textFile(args(0)).flatMap((s : String) => { func1(s) });
    val tokenized = sc.textFile(args(0)).map(func3).reduceByKey(func4);

    tokenized.collect().foreach(a => {
      System.out.println("In collect")
      System.out.println("Key = " + a._1)
      var sList = a._2
      var strCur = ""
      for (strCur <- sList) {
        System.out.println("strCur = " + strCur)
      }

    })
    System.out.println(tokenized.collect().mkString(", "))

    // count the occurrence of each word
    //    val wordCounts = tokenized.map(func2).reduceByKey(_ + _)

    // filter out words with fewer than threshold occurrences
    //    val filtered = wordCounts.filter(_._2 >= threshold)

    // count characters
    //    val charCounts = filtered.flatMap(_._1.toCharArray).map((_, 1)).reduceByKey(_ + _)


    //    System.out.println(charCounts.collect().mkString(", "))

    //    System.out.println("###########################Finished###########################")
  }
}
