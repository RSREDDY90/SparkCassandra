package com.spark.wc.SparkCassandraTest

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra.CassandraSQLContext

object CassandraTest {
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf()
      .setAppName("Test")
      .setMaster("local[*]")
      .set("spark.cassandra.connection.host", "localhost")
      .set("spark.cassandra.connection.port", "9042")
      .set("spark.driver.allowMultipleContexts", "true")
      .setSparkHome("${SPARK_HOME}")
      //.set("username", "cassandra")
     //.set("password", "password")
      
     // val conf = new SparkConf(true).set("spark.cassandra.connection.host", "0.0.0.0").setMaster("local[2]").setAppName("CSTest").set("spark.cassandra.connection.port", "9160")
        

    /** Connect to the Spark cluster: */
    val sc = new SparkContext(conf)
    
   // println(res.collect().size)

    //val user_table = sc.cassandraTable("system", "batches")
    
    val tablerdd = sc.cassandraTable("spark", "emp")
    
     val cc = new CassandraSQLContext(sc)
    
    val res =  cc.sql("select * from spark.emp")
    
  }
}