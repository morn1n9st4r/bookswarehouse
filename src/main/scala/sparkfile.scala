import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}


object sparkfile {

  def main(args: Array[String]):Unit = {

    val spark = SparkSession.builder()
      .appName("CreateDataFrameExample")
      .master("local[*]")
      .getOrCreate()

    val schema = StructType(
      Array(
        StructField("name", StringType, nullable = false),
        StructField("age", IntegerType, nullable = false)
      )
    )

    val data = Seq(("John", 30), ("Jane", 25), ("Bob", 40))
    val df: DataFrame = spark.createDataFrame(data).toDF(schema.fieldNames: _*)

    print(df)
    df.show()
    df.printSchema()
    
    spark.stop()
  }
}