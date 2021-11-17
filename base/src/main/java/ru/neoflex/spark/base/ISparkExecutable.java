package ru.neoflex.spark.base;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

public interface ISparkExecutable {
    void run(String name, SparkSession spark, JavaSparkContext sc, Map<String, String> jobParameters) throws Exception;
}
