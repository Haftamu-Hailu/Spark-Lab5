import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.janino.Java;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Exercise_2 {

    public static String groupByAndAgg(JavaSparkContext ctx) {
        String out = "";

        JavaRDD<String> winesRDD = ctx.textFile("src/main/resources/wines.10m.txt");
        //return the sum of attribute at position 5 grouped by type.
        //Here we are creating PairRDD() using scala helper methods
        //mapToPair. It is similar to map transformation; however,
        // this transformation produces PairRDD ,
        // that is, an RDD consisting of key and value pairs.
        // This transformation is specific to Java RDDs.
        // With other RDDs, map transformation can perform both ( map and mapToPair() ) of the tasks.

        JavaPairRDD<String, Double> version1=winesRDD.mapToPair(f->new Tuple2<>(f.split(",")[0],
                Double.parseDouble(f.split(",")[4])));

        JavaPairRDD<String, Double> result=version1.reduceByKey((a,b)->a+b);
        out+=result.collect();

        /*List<Tuple2<String,Double>> vers1 = winesRDD
                .mapToPair(f -> new Tuple2<String, Double>(f.split(",")[0],Double.parseDouble(f.split(",")[4])))
                .reduceByKey((f1,f2) -> f1+f2)
                .collect();

        out += "groupByAndAggregation\n";
        out += Arrays.toString(vers1.toArray());

        return out;*/
        return out;
    }

}

