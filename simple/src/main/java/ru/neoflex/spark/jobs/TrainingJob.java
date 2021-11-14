package ru.neoflex.spark.jobs;

import com.google.auto.service.AutoService;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import ru.neoflex.spark.base.ISparkJob;
import ru.neoflex.spark.base.SparkJobBase;
import ru.neoflex.spark.base.Utils;

import java.util.Map;

@AutoService(ISparkJob.class)
public class TrainingJob extends SparkJobBase {
    @Override
    public void run(SparkSession spark, JavaSparkContext sc, Map<String, String> jobParameters) throws Exception {
        info("Hello from spark ${version}",
                jobParameters, "version", spark.version());
        jobParameters.forEach((k, v) -> info("${key}: ${value}", "key", k, "value", v));
    }
}
