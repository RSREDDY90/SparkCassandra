package com.spark.cassandra.examples

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.datastax.spark.connector._
object SparkCassandraTest {

  def main(args: Array[String]): Unit = {

    /*val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1")
    val sc = new SparkContext("local[*]", "test", conf)*/

    val conf = new SparkConf()
      .setAppName("Test")
      .setMaster("local[2]")
      .set("spark.cassandra.connection.host", "127.0.0.1")
      .set("spark.cassandra.connection.port", "9042")
      .set("spark.driver.allowMultipleContexts", "true")

    /** Connect to the Spark cluster: */
    lazy val sc = new SparkContext(conf)

    val user_table = sc.cassandraTable("spark", "emp")

  }
}