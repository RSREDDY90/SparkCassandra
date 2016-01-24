package com.spark.wc.SparkCassandraTest

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra.CassandraSQLContext
import com.datastax.spark.connector.cql.CassandraConnector


object CassandraTableTest2 {
  
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
   
    
    val c = CassandraConnector(sc.getConf)
   
    //insert into table
    //c.withSessionDo ( session => session.execute("INSERT INTO spark.emp (emp_id, emp_name, emp_cmpny, emp_sal, div) VALUES(324147,'rsr', 'WIPRO', 121212, 'PES');"))
    
   val list = c.withSessionDo ( session => session.execute("select * from spark.emp;") ).all().toArray()
    
 
   list.foreach { println}
    
  
    
  
  }
}