import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{StringType, LongType, IntegerType}

object SilverAuthorsTransformations extends App {
      
    val spark = SparkSession.builder()
        .appName("Silver Autors Transformations")
        .master("local[*]")
        .config("spark.jars", "/opt/airflow/jars/postgresql-42.6.0.jar")
        .getOrCreate()

    val driver = "org.postgresql.Driver"
    val url = "jdbc:postgresql://172.18.0.2:5432/airflow"
    val user = "airflow"
    val password = "airflow"

    def readTable(tableName: String) = spark.read
        .format("jdbc")
        .option("driver", driver)
        .option("url", url)
        .option("user", user)
        .option("password", password)
        .option("dbtable", s"bronze.$tableName")
        .option("header", "true")
        .load()
    
    val authors_df = readTable("authors_raw")
    
    authors_df.show()

    val dfReplaced = authors_df.distinct()
        .withColumn("originalname", when(col("originalname") === "", "unknown").otherwise(col("originalname")))
        .withColumn("liked", when(col("liked") === "null", 0).otherwise(col("liked")))
        .withColumn("neutral", when(col("neutral") === "null", 0).otherwise(col("neutral")))
        .withColumn("disliked", when(col("disliked") === "null", 0).otherwise(col("disliked")))
        .withColumn("favorite", when(col("favorite") === "null", 0).otherwise(col("favorite")))
        .withColumn("reading", when(col("reading") === "null", 0).otherwise(col("reading")))
        //.withColumn("authorid",col("authorid").cast(LongType))
        .withColumn("name",col("name").cast(StringType))
        .withColumn("originalname",col("originalname").cast(StringType))
        .withColumn("liked",col("liked").cast(IntegerType))
        .withColumn("neutral",col("neutral").cast(IntegerType))
        .withColumn("disliked",col("disliked").cast(IntegerType))
        .withColumn("favorite",col("favorite").cast(IntegerType))
        .withColumn("reading",col("reading").cast(IntegerType))

    dfReplaced.select("*").write
        .format("jdbc")
        .option("driver", driver)
        .option("url", url)
        .option("user", user)
        .option("password", password)
        .option("dbtable", "silver.authors")
        .option("header", "true")
        .mode(SaveMode.Overwrite)
        .save()
}
