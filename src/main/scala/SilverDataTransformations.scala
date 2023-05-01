import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions.{col, when, lower,avg, regexp_replace, regexp_extract}
import org.apache.spark.sql.types.{IntegerType, StringType, DoubleType, LongType}
import org.apache.spark.sql.expressions.Window

object SilverDataTransformations extends App {

    val spark = SparkSession.builder()
        .appName("Silver Books Transformations")
        .master("spark://spark:7077")
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
    
    val books_df = readTable("books_raw")
    books_df.show()

    //replacing "null" string with actual null value (or other value when applicable)
    val dfReplaced = books_df.distinct()
    .withColumn("isbn", when(col("isbn") === "['null']", null).otherwise(col("isbn")))
    .withColumn("isbn", regexp_replace(col("isbn"), "\\[|\\]", ""))
    .withColumn("isbn", regexp_replace(col("isbn"), "'", ""))
    .withColumn("pages", when(col("pages") === "null", null).otherwise(col("pages")))
    .withColumn("pages", when(col("pages").isNull, avg(col("pages")).over(Window.partitionBy("publisherid"))).otherwise(col("pages")))
    // 130x200 can be safely placed as it is standard for book size
    // possible upgrade: depending on publisher and series set specific value
    .withColumn("size", when(col("size") === "null", "130x200").otherwise(col("size")))
    // this column as a lot of dirty data that sould be edited
    .withColumn("size", regexp_replace(col("size"), "\\.|\\,|\\-|\\~|\\;|мм|mm", ""))
    .withColumn("size", regexp_replace(col("size"), " ", ""))
    .withColumn("size", regexp_replace(col("size"), "XХх×\\*", "x"))
    // if value has format "paper size (cover size)" leave only cover
    .withColumn("size", regexp_replace(col("size"), ".+\\(", ""))
    .withColumn("size", regexp_replace(col("size"), "\\).*", ""))
    // make one standard of cover type 
    // S for soft (also used as default in case of nulls)
    // H for hard
    .withColumn("covertype", when(col("covertype") === "null", "S").otherwise(col("covertype")))
    .withColumn("covertype", when(lower(col("covertype")) === "мягкая", "S").otherwise(col("covertype")))
    .withColumn("covertype", when(lower(col("covertype")) === "мягкий", "S").otherwise(col("covertype")))
    .withColumn("covertype", when(lower(col("covertype")) === "твёрдая", "H").otherwise(col("covertype")))
    .withColumn("covertype", when(lower(col("covertype")) === "твердый", "H").otherwise(col("covertype")))
    .withColumn("covertype", when(lower(col("covertype")) === "твердая", "H").otherwise(col("covertype")))    
    .withColumn("language", when(col("language") === "null", null).otherwise(col("language")))
    .withColumn("copiesissued", when(col("copiesissued") === "null", null).otherwise(col("copiesissued")))
    // generally, books do not have age restriction, however some editions place them
    // to be safe, we could put pg13 rating
    .withColumn("agerestrictions", when(col("agerestrictions") === "null", 13).otherwise(col("agerestrictions")))
    // replace null with save bet as "prose"
    .withColumn("genres", when(col("genres") === "['null']", "Проза").otherwise(col("genres")))
    .withColumn("genres", regexp_replace(col("genres"), "\\[|\\]", ""))
    .withColumn("genres", regexp_replace(col("genres"), "'", ""))
    // the book might be not translated (in original language) ant there is no way to check it
    // other than using other librarie's API and check author's origin and language of book 
    .withColumn("translatorname", when(col("translatorname") === "null", "unknown").otherwise(col("translatorname")))
    .withColumn("translatorname", regexp_replace(col("translatorname"), "\\[|\\]", ""))
    .withColumn("translatorname", regexp_replace(col("translatorname"), "'", ""))
    .withColumn("translatorname", regexp_replace(col("translatorname"), " ", " "))
    .withColumn("translatorname", regexp_replace(col("translatorname"), "\\d|\\-|стр\\.|роман", ""))
    .withColumn("translatorname", regexp_replace(col("translatorname"), "\\sи\\s", ",\\s"))
    // replace comma with dot in doubles
    .withColumn("rating", regexp_replace(col("rating"), "\\,", "\\."))
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
        .withColumn("id",regexp_extract(col("id"), "\\d+", 0).cast(IntegerType))
        .withColumn("booktitle",col("booktitle").cast(StringType))
        .withColumn("Author",col("Author").cast(StringType))
        .withColumn("authorid",regexp_extract(col("authorid"), "\\d+", 0).cast(IntegerType))
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
        .withColumn("reviews",col("reviews").cast(IntegerType))
        .withColumn("quotes",col("quotes").cast(IntegerType))
        .withColumn("series",col("series").cast(StringType))
        .withColumn("publisherid",regexp_extract(col("publisherid"), "\\d+", 0).cast(IntegerType))
    
    dfReplacedProperTypes.printSchema()
    dfReplacedProperTypes.show()

    dfReplacedProperTypes.select("*").write
        .format("jdbc")
        .option("driver", driver)
        .option("url", url)
        .option("user", user)
        .option("password", password)
        .option("dbtable", "silver.books")
        .option("header", "true")
        .mode(SaveMode.Overwrite)
        .save()
}