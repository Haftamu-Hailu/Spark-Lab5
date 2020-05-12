import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Exercise_5 {

    @SuppressWarnings("Duplicates")
    public static String kNN_prediction(JavaSparkContext ctx) {
        String out = "";

        //Reading the heart.csv textFile into RDD
        JavaRDD<String> heartRDD = ctx.textFile("src/main/resources/heart.csv");

        //Filtering the TRAIN dataset from heart.csv
        JavaRDD<String> trainRDD = heartRDD.filter( f -> {
            String[] arrayValues = f.split(",");
            String datasetVal = Utils_1NN.getAttribute(arrayValues, "dataset");
            return datasetVal.equalsIgnoreCase("TRAIN");
        });

        //Filtering the TEST dataset from heart.csv
        JavaRDD<String> testRDD = heartRDD.filter( f -> {
            String[] arrayValues = f.split(",");
            String datasetVal = Utils_1NN.getAttribute(arrayValues, "dataset");
            return datasetVal.equalsIgnoreCase("TEST");
        });

        // Cartesian product between train and test rdds
        JavaPairRDD<String, String> cartesianRDD = testRDD.cartesian(trainRDD);

        // Calculating Euclidean distance between each pair of test and train values.
        JavaPairRDD<String, String> distRDD = cartesianRDD.mapToPair(f -> {
            String test = f._1;
            String train = f._2;
            String[] trainValues = train.split(",");
            String trainDiagnosisValue = Utils_1NN.getAttribute(trainValues, "diagnosis");
            Double dist = Utils_1NN.distance(test, train);
            return new Tuple2<>(test, trainDiagnosisValue + "_" + dist);

        });

        //Calculating the minimum euclidean distance.
        JavaPairRDD<String, String> minDistRDD = distRDD.reduceByKey((a,b) -> {
            double aDist = Double.parseDouble(a.split("_")[1]);
            double bDist = Double.parseDouble(b.split("_")[1]);

            if (aDist < bDist){
                return a;
            }
            return b;
        });

        // Finding the predicted value
        JavaPairRDD<String, Integer> predictionRDD = minDistRDD.mapToPair(f -> {
            String first = f._1;
            String second = f._2;
            String[] firstArrayValues = first.split(",");
            String testDiagnosisValue = Utils_1NN.getAttribute(firstArrayValues, "diagnosis");
            return new Tuple2<>(testDiagnosisValue + "_" + second.split("_")[0], 1);

        });

        //Generating the final confusion matrix
        JavaPairRDD<String, Integer> confusionMatrixRDD = predictionRDD.reduceByKey((a,b) -> a+b);

//        confusionMatrixRDD.sortByKey().foreach(value -> {System.out.println(value);});

        out += confusionMatrixRDD.collect();
        return out;
    }

}

