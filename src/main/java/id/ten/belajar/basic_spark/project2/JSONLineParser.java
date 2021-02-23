package id.ten.belajar.basic_spark.project2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JSONLineParser {

    public void parseJsonLines() {
        SparkSession spark = SparkSession.builder()
                .appName("JSON Lines to Dataframe")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("json")
                .load("src/main/resources/simple.json");

        Dataset<Row> df2 = spark.read().format("json")
                .option("multiline", true)
                .load("src/main/resources/multiline.json");

        df.show(5, 150);
        df.printSchema();

        df2.show(5, 150);
        df2.printSchema();

    }
}