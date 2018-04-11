package at.ac.ait

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ExtractionJob {
  
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Ransomware Dataset Extraction")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    val ransomwareDataset = new RansomwareDataset(spark)
    ransomwareDataset.extract()

    spark.stop()
    

  }

}