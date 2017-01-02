package com.cloudera.sparkwordcount

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import scala.collection.mutable.ListBuffer


object ScalaCSVGroupCalculator {
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

  def func3(s: String): Tuple2[Int,Array[Int]] = {
    System.out.println("In func3")
    System.out.println(s)

    var strList = s.split(",")
    var intList = new Array[Int](strList.size)
    var str = ""
    var i = 0
    for(i <- 0 until strList.length) {
      intList(i) = strList(i).toInt
    }


    return Tuple2(intList(0),intList)
  }

  def func4(t1: Array[Int], t2 : Array[Int]): Array[Int] = {
    System.out.println("In func4")

    var intList = new Array[Int](t1.size)

    for(i <- 0 until t1.length) {
      intList(i) = t1(i) + t2(i)
    }

    return intList
  }

  def func5(t: Tuple2[Int,Array[Int]]): Tuple2[Int,Array[Int]] = {
    System.out.println("In func5")

    var intList = new Array[Int](t._2.size)

    for(i <- 0 until t._2.size) {

    }

    return t
  }

  def main(args: Array[String]) {
    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))

    // get threshold
    val threshold = args(1).toInt

    // read in text file and split each document into words
    //val tokenized = sc.textFile(args(0)).flatMap((s : String) => { func1(s) });
    val tokenized = sc.textFile(args(0)).map(func3).groupByKey()

    tokenized.collect().foreach(a => {
      System.out.println("In collect")
      System.out.println("Key = " + a._1)
      var listOfList = a._2

      for (iList <- listOfList) {
        System.out.println("Printing List")
        for (num <- iList) {
          System.out.print(num + ",")
        }
        System.out.println()
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
