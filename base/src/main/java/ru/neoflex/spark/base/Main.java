package ru.neoflex.spark.base;

import java.util.ServiceLoader;

public class Main {
    public static void main(String[] args) {
        ServiceLoader<ISparkJob> loader = ServiceLoader.load(ISparkJob.class);
        SparkRuntime sparkRuntime = new SparkRuntime();
        for (ISparkJob job: loader) {
            sparkRuntime.registerJob(job);
        }
        sparkRuntime.run(args);
    }
}
