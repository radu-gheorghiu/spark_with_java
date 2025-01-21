package L2_Spark_Java_Dataset_API;

import L2_Spark_Java_Dataset_API.InferSchema;

public class Application {
    public static void main(String[] args){

        InferSchema parser = new InferSchema();
        parser.printSchema();

        System.out.print("================================================= Defined Schema:\n");

        DefinedSchema parser2 = new DefinedSchema();
        parser2.printDefinedSchema();

        System.out.print("================================================= JSON Parsing:\n");

        JSONLineParser parser3 = new JSONLineParser();
        parser3.parseJsonLines();
    }

}
