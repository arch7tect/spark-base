package ru.neoflex.spark.jobs;

import com.google.auto.service.AutoService;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import ru.neoflex.spark.base.ISparkJob;
import ru.neoflex.spark.base.SparkJobBase;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@AutoService(ISparkJob.class)
public class SimpleJob2 extends SparkJobBase {

    @Override
    public void run(SparkSession spark, JavaSparkContext sc, Broadcast<Map<String, String>> jobParameters) {
        List<Integer> data = IntStream.range(0, 100).boxed().collect(Collectors.toList());
        Dataset<Row> df = spark.createDataset(data, Encoders.INT()).coalesce(3).toDF("id");
        String file = jobParameters.getValue().getOrDefault("file", "simple.json");
        df.write().mode(SaveMode.Overwrite).json("/data/" + file);
    }
}
