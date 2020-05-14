import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.List;
import java.util.stream.Collectors;

public class Exercise_2 {

    public static String groupByAndAgg(JavaSparkContext ctx) {
        String out=" ";
        JavaRDD<String> report_2015 = ctx.textFile("src/main/resources/2015_long.csv");
        JavaRDD<String> report_2016 = ctx.textFile("src/main/resources/2016_long.csv");
        JavaRDD<String> report_2017 = ctx.textFile("src/main/resources/2017_long.csv");
        JavaRDD<String> FilterEurope = report_2015.union(report_2016).union(report_2017).
                filter(f -> !f.contains("Country")).filter(f -> f.contains("Europe")).cache();

        List<Tuple2<Double, String>> ranking = FilterEurope.map(f->f.split(","))
                .mapToPair(f -> new Tuple2<>(Double.parseDouble(f[5]), f[0]))
                .sortByKey(false).collect();

        List<String> topK = Lists.newArrayList();
        int i = 0;
        while (i < ranking.size() && topK.size() < 10) {
            if (ranking.get(i) != null) {
                String country = ranking.get(i)._2;
                if (!topK.contains(country)) {
                    topK.add(country);
                }
            }
            i++;
        }
        out+=topK.stream().collect(Collectors.joining(" \n "));
        return out;
    }

}
