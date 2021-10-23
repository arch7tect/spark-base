package ru.neoflex.spark.base;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.*;

public class SparkRuntime {
    private static final Logger logger = LogManager.getLogger(SparkRuntime.class);
    private final List<ISparkJob> jobs = new ArrayList<>();
    private final Map<String, String> params = new HashMap<>();
    private final Map<String, ISparkJob> registry = new HashMap<>();
    private String master;
    private Boolean hiveSupport = false;

    public SparkRuntime() {
        ServiceLoader<ISparkJob> loader = ServiceLoader.load(ISparkJob.class);
        for (ISparkJob job: loader) {
            registerJob(job);
        }
    }

    public void registerJob(ISparkJob job) {
        registry.put(job.getJobName(), job);
    }

    private SparkSession.Builder initBuilder(SparkSession.Builder builder) {
        builder.appName(jobs.get(0).getJobName());
        if (master != null) {
            builder.master(master);
        }
        if (hiveSupport) {
            builder.enableHiveSupport();
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
            try {
                JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
                Map<String, String> jobParameters = sc.broadcast(params).getValue();
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
        for (int i = 0; i < args.length; ++i) {
            String arg = args[i];
            if (arg.equals("-m")) {
                master = args[++i];
            }
            else if (arg.equals("-h")) {
                hiveSupport = !hiveSupport;
            }
            else if (arg.equals("-p")) {
                String param =args[++i];
                String[] parts = param.split("=", 2);
                if (parts.length != 2) {
                    throw new IllegalArgumentException(String.format("Parameter must be <key=value>, but found <%s>", param));
                }
                params.put(parts[0], parts[1]);
            }
            else {
                ISparkJob job = registry.get(arg);
                if (job == null) {
                    throw new IllegalArgumentException(String.format("Job <%s> not found", arg));
                }
                jobs.add(job);
            }
        }
    }
}
