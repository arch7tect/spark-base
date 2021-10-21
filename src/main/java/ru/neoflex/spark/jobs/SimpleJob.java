package ru.neoflex.spark.jobs;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import ru.neoflex.spark.SparkJobBase;
import scala.Tuple1;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SimpleJob extends SparkJobBase {
    @Override
    public String getJobName() {
        return "simple";
    }

    @Override
    public void run(SparkSession spark, JavaSparkContext sc, Broadcast<Map<String, String>> jobParameters) {
        var data = IntStream.range(0, 100).boxed().collect(Collectors.toList());
        var df = spark.createDataset(data, Encoders.INT()).coalesce(3).toDF("id");
        var file = jobParameters.getValue().getOrDefault("file", "simple.json");
        df.write().mode(SaveMode.Overwrite).json("/data/" + file);
    }
}
