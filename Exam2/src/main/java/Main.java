import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;


public class Main {
	
	public static void main(String[] args) throws Exception {
		SparkConf conf = new SparkConf().setAppName("SparkTraining").setMaster("local[*]");
        JavaSparkContext ctx = new JavaSparkContext(conf);
        ctx.setLogLevel("OFF");
		
		if (args.length < 1) {
			throw new Exception("Wrong number of parameters, usage: (exercise1,exercise2,exercise3)");
		}

		if (args[0].equals("exercise1")) {
            System.out.println(Exercise_1.optimize(ctx));
        }
		else if (args[0].equals("exercise2")) {
		    System.out.println(Exercise_2.groupByAndAgg(ctx));
        }


		else {
			throw new Exception("Wrong number of exercise");
		}
	}
}

