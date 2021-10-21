package ru.neoflex.spark;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkRuntime {
    private final Logger logger = LogManager.getLogger(SparkRuntime.class);
    private final List<SparkJobBase> jobs = new ArrayList<>();
    private final Map<String, String> params = new HashMap<>();

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
                throw new IllegalArgumentException("Job to run is not specified");
            }
            var spark = initBuilder(SparkSession.builder()).getOrCreate();
            var sc = new JavaSparkContext(spark.sparkContext());
            var jobParameters = sc.broadcast(params);
            try {
                for (var job: jobs) {
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
        for (var arg: args) {
            String[] parts = arg.split("=", 2);
            if (parts.length == 1) {
                String jobClass = parts[0];
                logger.info(String.format("instance of <%s> is being creating", jobClass));
                try {
                    var klass = Class.forName(jobClass);
                    jobs.add((SparkJobBase) klass.getConstructor().newInstance());
                } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
            }
            else {
                params.put(parts[0], parts[1]);
            }
        }
    }
}
