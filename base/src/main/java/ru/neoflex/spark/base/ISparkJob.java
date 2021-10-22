package ru.neoflex.spark.base;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

public interface ISparkJob {
    String getJobName();
    void run(SparkSession spark, JavaSparkContext sc, Broadcast<Map<String, String>> jobParameters);
}
