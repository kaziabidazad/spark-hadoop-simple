/**
 * 
 */
package duke.learn.spark;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * @author Kazi
 *
 */
public class SimpleApp {

    /**
     * @param args
     */
    public static void main(String[] args) {
	String logFile = "/home/duke/works/hadoop-spark/hadoop-installation/hadoop-3.3.4/README.md";
	SparkSession spark = SparkSession.builder().appName("Simple Spark App").config("spark.master", "local")
		.getOrCreate();
	Dataset<String> logDataset = spark.read().textFile(logFile).cache();

	long numAs = logDataset.filter((FilterFunction<String>) s -> s.contains("a")).count();
	long numBs = logDataset.filter((FilterFunction<String>) s -> s.contains("b")).count();

	System.out.printf("Lines with a: %d, lines with b: %d \n", numAs, numBs);
    }

}
