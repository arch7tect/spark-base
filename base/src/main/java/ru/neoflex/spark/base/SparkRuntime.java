package ru.neoflex.spark.base;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SparkRuntime {
    private final Logger logger = LogManager.getLogger(SparkRuntime.class);
    private final List<ISparkJob> jobs = new ArrayList<>();
    private final Map<String, String> params = new HashMap<>();
    private final Map<String, ISparkJob> registry = new HashMap<>();

    public void registerJob(ISparkJob job) {
        registry.put(job.getJobName(), job);
    }

    private SparkSession.Builder initBuilder(SparkSession.Builder builder) {
        builder.appName(jobs.get(0).getJobName());
        if (params.containsKey("master")) {
            builder.master(params.get("master"));
        }
        return builder;
    }

    public void run(String[] args) {
        try {
            parseArgs(args);
            if (jobs.isEmpty()) {
                if (registry.size() == 1) {
                    jobs.add(registry.values().stream().findFirst().get());
                }
                else {
                    throw new IllegalArgumentException("Job to run is not specified");
                }
            }
            SparkSession spark = initBuilder(SparkSession.builder()).getOrCreate();
            JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
            Broadcast<Map<String, String>> jobParameters = sc.broadcast(params);
            try {
                for (ISparkJob job: jobs) {
                    logger.info(String.format("Running job <%s>", job.getJobName()));
                    job.run(spark, sc, jobParameters);
                }
            }
            finally {
                spark.stop();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void parseArgs(String[] args) {
        for (String arg: args) {
            String[] parts = arg.split("=", 2);
            if (parts.length == 1) {
                String jobName = parts[0];
                ISparkJob job = registry.get(jobName);
                if (job == null) {
                    throw new IllegalArgumentException(String.format("Job <%s> not found", jobName));
                }
                jobs.add(job);
            }
            else {
                params.put(parts[0], parts[1]);
            }
        }
    }
}
