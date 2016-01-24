package com.spark.wc.SparkCassandraTest;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class Create_Data {

	public static void main(String[] args) {
		String query1 = "INSERT INTO emp (emp_id, emp_name, emp_cmpny, emp_sal, div)"
				
         + " VALUES(324146,'vamsi', 'WIPRO', 1999212, 'PES');" ;
                             

      //Creating Cluster object
      Cluster cluster = Cluster.builder().addContactPoint("localhost").build();
      cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(1000);
      //Creating Session object
      Session session = cluster.connect("spark");
       
      //Executing the query
      session.execute(query1);
        
      System.out.println("Data created");

	}

}
