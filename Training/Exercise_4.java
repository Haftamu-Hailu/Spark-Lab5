import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Exercise_4 {

    /** We perform aggregation
     * SELECT native_country, SUM(capital_gain) FROM adults GROUP BY native_country
     */
    public static String groupByAndAgg(JavaSparkContext ctx) {
        String out = "";
        JavaRDD<String> adultRDD = ctx.textFile("src/main/resources/adult.1000.csv");

    JavaPairRDD<String,Integer>MapCountry=adultRDD.mapToPair(f->
        new Tuple2<String, Integer>(f.split(",")[12], Integer.parseInt(f.split(",")[9])));

     JavaPairRDD<String,Integer>result=MapCountry.reduceByKey((f1,f2)->f1+f2);
     out+=result.collect();
        return out;



    }

}

