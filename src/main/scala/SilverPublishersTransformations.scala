import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions.{col, when, avg, regexp_replace, regexp_extract}
import org.apache.spark.sql.types.{StringType, LongType, IntegerType}
import org.apache.spark.sql.expressions.Window

object SilverPublishersTransformations extends App {
        
    val spark = SparkSession.builder()
        .appName("Silver Publishers Transformations")
        .master("local[*]")
        //.master("spark://spark:7077")
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
    
    val publishers_df = readTable("publishers_raw")
    
    publishers_df.show()

    val dfReplaced = publishers_df.dropDuplicates("publisherid")
        .withColumn("name", regexp_replace(col("name"), "\\«|\\»|Издательство", ""))
        .withColumn("years", when(col("years").isNull, avg(col("years")).over(Window.partitionBy()).cast(IntegerType)).otherwise(col("years")))
        .withColumn("page", when(col("page") === "null", "none").otherwise(col("page")))
        .withColumn("books", regexp_replace(col("books"), "\\(|\\)|Книги", ""))
        .withColumn("favorite", regexp_replace(col("favorite"), "\\(|\\)", ""))
        .withColumn("favorite", when(col("favorite").isNull, 0).otherwise(col("favorite")))
        .withColumn("publisherid",regexp_extract(col("publisherid"), "\\d+", 0).cast(IntegerType))
        .withColumn("name",col("name").cast(StringType))
        .withColumn("books",col("books").cast(IntegerType))
        .withColumn("years",col("years").cast(IntegerType))
        .withColumn("page",col("page").cast(StringType))
        .withColumn("favorite",col("favorite").cast(IntegerType))

    
    dfReplaced.dropDuplicates("publisherid").select("*").write
        .format("jdbc")
        .option("driver", driver)
        .option("url", url)
        .option("user", user)
        .option("password", password)
        .option("dbtable", "silver.publishers")
        .option("header", "true")
        .mode(SaveMode.Overwrite)
        .save()
}
