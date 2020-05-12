import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Exercise_1 {
	
	public static String basicAnalysis(JavaSparkContext ctx) {
		String out = "";
		
		JavaRDD<String> winesRDD = ctx.textFile("src/main/resources/wines.10m.txt");


		out += "The file has "+winesRDD.count()+" lines\n";
		out += "#################################\n";
		
		out += "The first ten lines have the following content:\n";
		for (String line : winesRDD.take(10)) {
			out += "	"+line;
			out += "\n";
		}
		out += "#################################\n";

		JavaRDD<Double> fourthAttribute = winesRDD.map(f -> Double.parseDouble(f.split(",")[3])).sortBy(f -> f,true,2);
		out += "For the sixth attribute the maximum value is "+fourthAttribute.first()+"\n";
		out += "#################################\n";

		JavaRDD<String> type1 = winesRDD.filter(f -> f.contains("type_1"));
		out += type1.count()+" elements are of type 1\n";
		out += "#################################\n";
		JavaRDD<String> type2 = winesRDD.filter(f -> f.contains("type_2"));
		out += type2.count()+" elements are of type 2\n";
		out += "#################################\n";
		JavaRDD<String> type3 = winesRDD.filter(f -> f.contains("type_3"));
		out += type3.count()+" elements are of type 3\n";
		out += "#################################\n";
		return out;
	}
}

