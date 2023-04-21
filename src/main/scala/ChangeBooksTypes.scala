import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{col, when, lower}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DoubleType

object ChangeBooksTypes extends App {

    val spark = SparkSession.builder()
    .appName("DedublicateBooks")
    .master("local[*]")
    .getOrCreate()

    /*case class Book(
        val id: Int,
        val booktitle: String,
        val Author: String,
        val authorid: Int,
        val isbn: String,
        val editionyear: Int,
        val pages: Int,
        val size: String,
        val covertype: String,
        val language: String,
        val copiesissued: String,
        val agerestrictions: Int,
        val genres: String,
        val translatorname: String,
        val rating: Double,
        val haveread: Int,
        val planned: Int,
        val reviews: Int,
        val quotes: Int,
        val series: String,
        val publisherid: Int
    )*/

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
    
    val books_df = readTable("books_raw")

    //replacing "null" string with actual null value (or other value when applicable)
    val dfReplaced = books_df
    .withColumn("isbn", when(col("isbn") === "['null']", null).otherwise(col("isbn")))
    .withColumn("pages", when(col("pages") === "null", null).otherwise(col("pages")))
    // make one standard of cover type 
    // S for soft (also used as default in case of nulls)
    // H for hard
    .withColumn("covertype", when(col("covertype") === "null", "S").otherwise(col("covertype")))
    .withColumn("covertype", when(lower(col("covertype")) === "мягкая", "S").otherwise(col("covertype")))
    .withColumn("covertype", when(lower(col("covertype")) === "мягкий", "S").otherwise(col("covertype")))
    .withColumn("covertype", when(lower(col("covertype")) === "твёрдая", "H").otherwise(col("covertype")))
    .withColumn("covertype", when(lower(col("covertype")) === "твердый", "H").otherwise(col("covertype")))
    .withColumn("language", when(col("language") === "null", null).otherwise(col("language")))
    .withColumn("copiesissued", when(col("copiesissued") === "null", null).otherwise(col("copiesissued")))
    // generally, books do not have age restriction, however some editions place them
    // to be safe, we could put pg13 rating
    .withColumn("agerestrictions", when(col("agerestrictions") === "null", 13).otherwise(col("agerestrictions")))
    .withColumn("genres", when(col("genres") === "['null']", null).otherwise(col("genres")))
    // the book might be not translated (in original language) ant there is no way to check it
    // other than using other librarie's API and check author's origin and language of book 
    .withColumn("translatorname", when(col("translatorname") === "null", "unknown\\original").otherwise(col("translatorname")))
    // if the book is new, we can put 0 values
    .withColumn("rating", when(col("rating") === "null", 0).otherwise(col("rating")))
    .withColumn("haveread", when(col("haveread") === "null", 0).otherwise(col("haveread")))
    .withColumn("planned", when(col("planned") === "null", 0).otherwise(col("planned")))
    .withColumn("reviews", when(col("reviews") === "null", 0).otherwise(col("reviews")))
    .withColumn("quotes", when(col("quotes") === "null", 0).otherwise(col("quotes")))
    // certain books can have no series and be standalone
    .withColumn("series", when(col("series") === "null", "Standalone").otherwise(col("series")))
    .withColumn("publisherid", when(col("publisherid") === "null", null).otherwise(col("publisherid")))

    val dfReplacedProperTypes = dfReplaced
        .withColumn("id",col("id").cast(IntegerType))
        .withColumn("booktitle",col("booktitle").cast(StringType))
        .withColumn("Author",col("Author").cast(StringType))
        .withColumn("authorid",col("authorid").cast(IntegerType))
        .withColumn("isbn",col("isbn").cast(StringType))
        .withColumn("editionyear",col("editionyear").cast(IntegerType))
        .withColumn("pages",col("pages").cast(IntegerType))
        .withColumn("size",col("size").cast(StringType))
        .withColumn("covertype",col("covertype").cast(StringType))
        .withColumn("language",col("language").cast(StringType))
        .withColumn("copiesissued",col("copiesissued").cast(StringType))
        .withColumn("agerestrictions",col("agerestrictions").cast(IntegerType))
        .withColumn("genres",col("genres").cast(StringType))
        .withColumn("translatorname",col("translatorname").cast(StringType))
        .withColumn("rating",col("rating").cast(DoubleType))
        .withColumn("language",col("language").cast(StringType))
        .withColumn("planned",col("planned").cast(IntegerType))
        .withColumn("reviews",col("reviews").cast(StringType))
        .withColumn("quotes",col("quotes").cast(IntegerType))
        .withColumn("series",col("series").cast(StringType))
        .withColumn("publisherid",col("publisherid").cast(IntegerType))
    
    dfReplacedProperTypes.printSchema()
}