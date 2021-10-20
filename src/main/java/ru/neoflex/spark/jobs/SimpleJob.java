package ru.neoflex.spark.jobs;

import org.apache.spark.sql.SparkSession;
import ru.neoflex.spark.SparkJobBase;

public class SimpleJob extends SparkJobBase {
    @Override
    public String getJobName() {
        return "simple";
    }

    @Override
    public void run(SparkSession spark) {

    }
}
