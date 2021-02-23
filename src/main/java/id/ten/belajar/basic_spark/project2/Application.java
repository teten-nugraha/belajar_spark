package id.ten.belajar.basic_spark.project2;

public class Application {
    public static void main(String[] args) {

//        InferCSVSchema parser = new InferCSVSchema();
//        parser.printSchema();

//        DefineCSVSchema parser2 = new DefineCSVSchema();
//        parser2.printDefinedSchema();

        JSONLineParser parser = new JSONLineParser();
        parser.parseJsonLines();
    }
}
