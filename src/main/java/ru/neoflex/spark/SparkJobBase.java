package ru.neoflex.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

public abstract class SparkJobBase {
    public abstract String getJobName();
    public abstract void run(SparkSession spark, JavaSparkContext sc, Broadcast<Map<String, String>> jobParameters);
}
