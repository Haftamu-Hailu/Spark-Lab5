package exercise_1;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

import static java.lang.Double.parseDouble;

public class Exercise_1 {

    public static String humanResources(JavaSparkContext ctx) {
        String out = "";
        JavaRDD<String> HRMRdd = ctx.textFile("src/main/resources/HR_comma_sep.csv");

        JavaPairRDD<String,Tuple2<Integer,Double>>filterRDD= HRMRdd.filter(f -> !f.contains("satisfaction_level")).mapToPair(f -> new Tuple2<>(
                f.split(",")[2], new Tuple2<>(1, parseDouble(f.split(",")[0]))));


        List<Tuple2<Double, String>> Average= filterRDD.reduceByKey((tuple1, tuple2) -> new Tuple2<>(tuple1._1 + tuple1._1, tuple1._2 + tuple1._2))
                .mapToPair(tuple -> new Tuple2<>(tuple._2._2 / tuple._2._1, tuple._1))
                .sortByKey()
                .collect();

        for (Tuple2<Double, String> av : Average) {
            out =  out+"Employees who work in " + av._2 + " project have an average satisfaction of " + av._1 + "\n";
        }
        return out;
    }
}