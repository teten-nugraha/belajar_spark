package id.ten.belajar.basic_spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

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

        df.show(3);

    }
}
