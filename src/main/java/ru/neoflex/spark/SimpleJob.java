package ru.neoflex.spark;

import org.apache.spark.sql.SparkSession;

public class SimpleJob extends SparkJobBase {
    @Override
    public String getJobName() {
        return "simple";
    }

    @Override
    public void run(SparkSession spark) {

    }
}
