package com.spark.wc.spark.cass.insert

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text


object InsertToCassandra {
  
  
  def main(args: Array[String]): Unit = {
    
     val conf = new SparkConf()
      .setAppName("InsertTest")
      .setMaster("local[*]")
      .set("spark.cassandra.connection.host", "localhost")
      .set("spark.cassandra.connection.port", "9042")
      .set("spark.driver.allowMultipleContexts", "true")
      .setSparkHome("${SPARK_HOME}")
      
        

    /** Connect to the Spark cluster: */
    val sc = new SparkContext(conf)

    val employeeDetails = sc.textFile("D:/Workspace/scala/SparkWordCount/input/employeeDetails.txt")

    val companyDetails = sc.textFile("D:/Workspace/scala/SparkWordCount/input/WiproEmpDetails")

    val empRDD = employeeDetails.map { x => x.split(" ") }.map { x => (x(4), x(1).concat("_") concat (x(2))) }

    val cmpRDD = companyDetails.map { x => x.split(" ") }.map { x => (x(0), (x(1), x(2), x(3), x(4), x(5))) }

    val empHist = empRDD.leftOuterJoin(cmpRDD)



    val cust1 = empHist.map{ y => ( y._1, y._2._1, y._2._2.map(f =>(f._1,f._2,f._3,f._4,f._5)).get.productIterator.toList.mkString(","))}
    
    cust1.foreach(f => inserToCassandra(f._1,f._2,f._3,sc))
    
    
  }
  
  
  def inserToCassandra(param1: String, param2: String,param3: String, sc:SparkContext): String = {

    
    val id:IntWritable = new IntWritable(Integer.valueOf(param1))
    val name:Text = new Text(param2)
    
    val param = param3.split(",")
    
     val cmpny:Text = new Text(param(0))
     val sal:IntWritable = new IntWritable(Integer.valueOf(param(1)))
     val div:Text = new Text(param(2))
    val client:Text = new Text(param(3))
    val proj_name:Text = new Text(param(4))
    
    
    println(param1+" "+param2+" "+param(0)+" "+param(1)+" "+param(2)+" "+param(3)+" "+param(4))
    
     val c = CassandraConnector(sc.getConf)
   
    //insert into table
//                                               /id int PRIMARY KEY, name text, cmpny text, sal varint, div text, client text, proj_name text
     val query = "INSERT INTO spark.emp_details (id, name, cmpny, sal, div,client,proj_name) VALUES("+id+",'"+name+"', '"+cmpny+"', "+sal+", '"+div+"','"+client+"'+'"+proj_name+"');";
    c.withSessionDo ( session => session.execute(query))
     
     
    return "Hello"

  }
  
  
  
}

