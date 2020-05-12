package exercise_2;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.List;


public class Exercise_2 {

    //The happiest country of Europe in 2015, 2016 and 2017 according to its happiness score
    public static String happinessRanking(JavaSparkContext spark) {
        String out = "";

        JavaRDD<String> report_2015 = spark.textFile("src/main/resources/2015_long.csv");
        JavaRDD<String> report_2016 = spark.textFile("src/main/resources/2016_long.csv");
        JavaRDD<String> report_2017 = spark.textFile("src/main/resources/2017_long.csv");

        JavaRDD<String> all = report_2015.union(report_2016).union(report_2017);

        JavaRDD<String> FilterEurope = all.
                filter(f -> !f.contains("Country")).filter(f -> f.contains("Europe")).cache();

        List<Tuple2<Double, String>> happiestcountry = FilterEurope.map(f->f.split(","))
                .mapToPair(f -> new Tuple2<>(Double.parseDouble(f[3]), f[0]))
                .sortByKey(false).collect();

        Tuple2<Double,String> top = happiestcountry.get(0);
        out = top._2.split(",")[0]+" is the happiest country in Europe for 2015, 2016 and 2017 with an score of "+top._1;

        return out;
    }


}
