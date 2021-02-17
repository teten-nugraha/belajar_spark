package id.ten.belajar.basic_spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

public class Application {
    public static void main(String[] args) {

        // create spark session
        SparkSession spark = new SparkSession.Builder()
                .appName("CSV to DB")
                .master("local")
                .getOrCreate();

        // get Data
        Dataset<Row> df = spark.read().format("csv")
                .option("header",true)
                .load("src/main/resources/name_and_comments.txt");

//        df.show(3);

        // transformation
        df = df.withColumn("full_name", concat(df.col("last_name"), lit(", "), df.col("first_name")));

        // transformation
        df = df.filter(df.col("comment").rlike("\\d+"))
                .orderBy(df.col("last_name"));
        df.show();

        String dbConnUrl = "jdbc:mysql://localhost:3306/belajar_spark";
        Properties prop = new Properties();
        prop.setProperty("driver", "com.mysql.jdbc.Driver");
        prop.setProperty("user", "root");
        prop.setProperty("password", "root");

        df.write()
                .mode(SaveMode.Overwrite)
                .jdbc(dbConnUrl, "project1", prop);
    }
}
