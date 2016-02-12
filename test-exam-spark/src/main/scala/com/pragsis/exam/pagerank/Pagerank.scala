package com.pragsis.exam.pagerank

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Pagerank extends App {
  
      // creating SparkContext 
      val conf = new SparkConf().setAppName("Page Rank web-Google file").setMaster("local[*]")
      val sc = new SparkContext(conf)
  
      // point to Google file
      val data = sc.textFile("file:///home/cloudera/Desktop/examen/test-exam-spark/src/main/resources/web-Google.txt")

      // creating links among pages from file using cache to improve performance
      val links = data.filter(line=>(!line.startsWith("#"))).map(s=>s.split("\\s+")).map(ids=>(ids(0),ids(1))).distinct().groupByKey().cache()
      
      // init rank for every page
      var ranks = links.mapValues(v => 1.0)  		

      // declare number iterations
      val numIter = 10

      // loop to calculate and update the rank for each page 
      // limited by numIter iterations	 	
      for (i <- 1 to numIter) {
          
          // join pages and ranks RDDs
	        val infopages = links.join(ranks)
	
	        // calculating ranks for each tuple (link,rank)
	        val inforanks = infopages.values.flatMap{ case (links, rank) => links.map(link => (link, rank / links.size)) }
	
	        // updating rank with formula given and using cache to update ranks
    	    ranks = inforanks.reduceByKey((x,y)=> x+y).mapValues(value=> value*0.85  + 0.15).cache()
    	    //ranks.leftOuterJoin(updatedranks)
      }

      // sort result pages by rank 
      val result = ranks.map(_.swap).sortByKey(false).map(_.swap)
    
      // save result as "resultado"
      result.saveAsTextFile("resultado")	

}
