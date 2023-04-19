import org.apache.spark.sql.{SparkSession, Encoder, Encoders}
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}


object sparkfile {

  case class Person(name: String, age: Int)
  
  def main(args: Array[String]):Unit = {

    val spark = SparkSession.builder()
      .appName("CreateDataFrameExample")
      .master("local[*]")
      .getOrCreate()


    val data = List(Person("John", 30), Person("Jane", 25), Person("Bob", 40))
    import spark.implicits._
    val df = spark.createDataset(data)

    df.show()
    df.printSchema()
    
    spark.stop()
  }
}