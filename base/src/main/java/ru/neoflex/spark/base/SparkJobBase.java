package ru.neoflex.spark.base;

public abstract class SparkJobBase implements ISparkJob {
    public String getJobName() {
        return this.getClass().getSimpleName();
    }
}
