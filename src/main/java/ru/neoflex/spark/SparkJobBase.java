package ru.neoflex.spark;

import org.apache.spark.sql.SparkSession;

public abstract class SparkJobBase {
    public abstract String getJobName();
    public abstract void run(SparkSession spark);
}
