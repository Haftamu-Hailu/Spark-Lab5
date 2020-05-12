import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Exercise_3_kNN {

    public static String kNN_prediction(JavaSparkContext ctx) {
        String out = "";

        JavaRDD<String> breast = ctx.textFile("src/main/resources/breast.csv");

        JavaRDD<String> data = breast.filter(t -> !t.contains("id")).cache();

        JavaRDD<String> train = data.filter(t -> Utils_1NN.getAttribute(t.split(","),"train").equals("1"));
        JavaRDD<String> test = data.filter(t -> Utils_1NN.getAttribute(t.split(","),"train").equals("0"));

        out = test.cartesian(train)
                .mapToPair(t -> new Tuple2<String, Tuple2<String,Double>>(t._1,new Tuple2<>(t._2,Utils_1NN.distance(t._1,t._2))))
                .groupByKey()
                .map(t -> {
                    double min = Double.MAX_VALUE;
                    String min_diagnosis = "";
                    for (Tuple2<String,Double> v : t._2) {
                        if (v._2 < min) {
                            min = v._2;
                            min_diagnosis = Utils_1NN.getAttribute(v._1.split(","),"diagnosis");
                        }
                    }
                    String truth = Utils_1NN.getAttribute(t._1.split(","),"diagnosis");
                    return min_diagnosis+"_"+truth;
                })
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b).collect().toString();

        return out;
    }

}

