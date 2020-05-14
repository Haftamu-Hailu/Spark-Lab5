import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

public class Exercise_1{
    public static String optimize(JavaSparkContext ctx) {
        String out = "";
        JavaRDD<String> bankRdd = ctx.textFile("src/main/resources/bank.csv");


        JavaRDD<String> HeaderlessRDD = bankRdd.filter(f -> !f.contains("\"age\"")).cache();
        //filter yes and no lines
        JavaRDD<String> noLoan = HeaderlessRDD.filter(f -> f.split(";")[7].equals("\"no\""));
        JavaRDD<String> withloan = HeaderlessRDD.filter(f -> f.split(";")[7].equals("\"yes\""));

        //create Pair RDD  of yes and no
        JavaPairRDD<String, Double> noLoanRDD = noLoan.mapToPair(f -> new Tuple2<String, Double>(f.split(";")[1], Double.parseDouble(f.split(";")[5])));
        JavaPairRDD<String, Double> withloanRDD = withloan.mapToPair(f -> new Tuple2<String, Double>(f.split(";")[1], Double.parseDouble(f.split(";")[5])));

        //Find Average of each
        JavaPairRDD<String, Double> NoloadAverage = noLoanRDD.mapValues(t -> new Tuple2<>(t, 1))
                .reduceByKey((tuple1, tuple2) -> new Tuple2<>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2)).mapValues(tuple -> tuple._1 / tuple._2);

        JavaPairRDD<String, Double> WithloandAverage = withloanRDD.mapValues(t -> new Tuple2<>(t, 1))
                .reduceByKey((tuple1, tuple2) -> new Tuple2<>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2)).mapValues(tuple -> tuple._1 / tuple._2);

        JavaPairRDD<String, Double> finalNoRDD = NoloadAverage.mapToPair(tuple -> new Tuple2<>(tuple._1, tuple._2));
        JavaPairRDD<String, Double> finalYesRDD = WithloandAverage.mapToPair(tuple -> new Tuple2<>(tuple._1, tuple._2));

        JavaPairRDD<String, String> JoinRDD = finalNoRDD.join(finalYesRDD).mapValues(t -> "avg balance with loan = " + t._2 + ", without loan = " + t._1).sortByKey();
        out += JoinRDD.map(t->{
            String first = t._1;
            String second = t._2;
            return (first +":" + second + "\n");
        }).collect();
        return out;
    }
}