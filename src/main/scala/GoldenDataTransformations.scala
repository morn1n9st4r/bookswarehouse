import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when, split, explode, collect_set, monotonically_increasing_id}
import org.apache.spark.sql.SaveMode


object GoldenDataTransformations extends App {

    val spark = SparkSession.builder()
        .appName("GoldenDataTransformations")
        .master("local[*]")
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
        .option("dbtable", s"silver.$tableName")
        .option("header", "true")
        .load()
    
    val books_df = readTable("books")
    
    val genres_with_occurences_df = books_df
        .withColumn("genre_arr", split(col("genres"), ","))
        .select(col("id"), explode(col("genre_arr")).as("genre"))
        .groupBy("genre")
        .agg(collect_set(col("id")).as("book_ids"))
    

    // separate column for genres
    val genres_with_id = genres_with_occurences_df
        .withColumn("id", monotonically_increasing_id).as("genre_id")

    val combinations_of_books_and_genres = genres_with_id
        .select(col("id").as("genre_id"), explode(col("book_ids")).as("book_id"))
        .orderBy("book_id")
    
    val genres_table = genres_with_id.drop("book_ids")

    genres_table.show()
    combinations_of_books_and_genres.show()


    genres_table.select("*").write
        .format("jdbc")
        .option("driver", driver)
        .option("url", url)
        .option("user", user)
        .option("password", password)
        .option("dbtable", "gold.genres")
        .option("header", "true")
        .mode(SaveMode.Overwrite)
        .save()
    
    combinations_of_books_and_genres.select("*").write
        .format("jdbc")
        .option("driver", driver)
        .option("url", url)
        .option("user", user)
        .option("password", password)
        .option("dbtable", "gold.book_genre")
        .option("header", "true")
        .mode(SaveMode.Overwrite)
        .save()
    
    val books_without_columns = books_df.drop("genres")
            .drop("Author")
            .drop("translatorname")
            .drop("copiesissued")

    books_without_columns.select("*").write
        .format("jdbc")
        .option("driver", driver)
        .option("url", url)
        .option("user", user)
        .option("password", password)
        .option("dbtable", "gold.books")
        .option("header", "true")
        .mode(SaveMode.Overwrite)
        .save()
}
