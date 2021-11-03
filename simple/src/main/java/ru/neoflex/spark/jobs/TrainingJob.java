package ru.neoflex.spark.jobs;

import com.google.auto.service.AutoService;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import ru.neoflex.spark.base.ISparkJob;
import ru.neoflex.spark.base.SparkJobBase;

import java.util.Map;

@AutoService(ISparkJob.class)
public class TrainingJob extends SparkJobBase {
    @Override
    public void run(SparkSession spark, JavaSparkContext sc, Map<String, String> jobParameters) throws Exception {
        info("Hello %s from spark %s", jobParameters.get("name"), spark.version());
        Dataset<Row> ds = spark.table("names_joined_ext");
        ds.show();
        info("Count: %d", ds.count());
    }
}
