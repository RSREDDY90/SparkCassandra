package com.spark.sql.intro

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext


object SparkSQLInsertTable {
  
  def main(args: Array[String]): Unit = {
    
    val sc = new SparkContext("local[2]", "sparkSQL", System.getenv("SPARK_HOME"))
    
    val sqlContext = new SQLContext(sc)

// Import statement to implicitly convert an RDD to a DataFrame

import sqlContext.implicits._
// Create a DataFrame of Customer objects from the dataset text file.
val dfCustomers = sc.textFile("D:/Workspace/scala/SparkWordCount/input/customers.txt").map(_.split(",")).map(p => Customer(p(0).trim.toInt, p(1), p(2), p(3), p(4))).toDF()


// Register DataFrame as a table.
dfCustomers.registerTempTable("customers")

// Display the content of DataFrame
dfCustomers.show()

// Print the DF schema
dfCustomers.printSchema()

// Select customer name column
dfCustomers.select("name").show()

// Select customer name and city columns
dfCustomers.select("name", "city").show()

// Select a customer by id
dfCustomers.filter(dfCustomers("customer_id").equalTo(500)).show()

// Count the customers by zip code
dfCustomers.groupBy("zip_code").count().show()
  }
  
  // Create a custom class to represent the Customer
case class Customer(customer_id: Int, name: String, city: String, state: String, zip_code: String)
  
}