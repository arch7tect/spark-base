package ru.neoflex.spark.base;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Map;

public interface ISparkJob extends Serializable {
    String getJobName();
    void run(SparkSession spark, JavaSparkContext sc, Map<String, String> jobParameters) throws Exception;
}
