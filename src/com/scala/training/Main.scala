package training

import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.SparkContext._

object Count
{
  
 
  def main(args : Array[String])
  {
    
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val conf = new SparkConf().setAppName("Count");
    
   val sc1 = new SparkContext(conf)
   
    val text = sc1.textFile("../jonuchauhan1/input/book.txt")
    
    val words  = text.flatMap(x =>(x.split(" "))).map(x=>(x,1))
    
    val counts = words.reduceByKey((x,y)=> (x+y))
     val k = counts.map(x=>(x._2.toInt,x._1))
    
    k.sortByKey(ascending=false).foreach(println)
  }
  
  
  
}