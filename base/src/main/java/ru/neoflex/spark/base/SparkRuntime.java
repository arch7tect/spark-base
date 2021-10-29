package ru.neoflex.spark.base;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.*;

public class SparkRuntime {
    private static final Logger logger = LogManager.getLogger(SparkRuntime.class);
    private final List<ISparkJob> jobs = new ArrayList<>();
    private final List<String> propertyFiles = new ArrayList<>();
    private final Map<String, String> params = new HashMap<>();
    private final Map<String, String> config = new HashMap<>();
    private final Map<String, ISparkJob> registry = new HashMap<>();
    private String master;
    private Boolean hiveSupport = false;

    public SparkRuntime() {
        ServiceLoader<ISparkJob> loader = ServiceLoader.load(ISparkJob.class);
        for (ISparkJob job: loader) {
            registry.put(job.getJobName(), job);
        }
    }

    private SparkSession.Builder initBuilder(SparkSession.Builder builder) {
        builder.appName(jobs.get(0).getJobName());
        if (master != null) {
            builder.master(master);
        }
        for (String key: config.keySet()) {
            builder.config(key, config.get(key));
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
                Map<String, String> paramsEffective = new HashMap<>();
                if (!propertyFiles.isEmpty()) {
                    readParamsFromFiles(sc, paramsEffective);
                }
                paramsEffective.putAll(params);
                Map<String, String> jobParameters = sc.broadcast(paramsEffective).getValue();
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

    private void readParamsFromFiles(JavaSparkContext sc, Map<String, String> paramsEffective) throws IOException {
        FileSystem fs = FileSystem.get(sc.hadoopConfiguration());
        for (String propertyFile: propertyFiles) {
            logger.info(String.format("Load parameters from file <%s>", propertyFile));
            try (FSDataInputStream is = fs.open(new Path(propertyFile))) {
                Properties props = new Properties();
                props.load(is);
                for (String key: props.stringPropertyNames()) {
                    paramsEffective.put(key, props.getProperty(key));
                }
            }
        }
    }

    private void parseArgs(String[] args) {
        for (int i = 0; i < args.length; ++i) {
            String arg = args[i];
            if (arg.equals("-m")) {
                if (++i >= args.length) {
                    throw new IllegalArgumentException("Master not specified for option -m");
                }
                master = args[i];
            }
            else if (arg.equals("-h")) {
                hiveSupport = !hiveSupport;
            }
            else if (arg.equals("-p")) {
                if (++i >= args.length) {
                    throw new IllegalArgumentException("Parameter not specified for option -p");
                }
                String param =args[i];
                String[] parts = param.split("=", 2);
                if (parts.length != 2) {
                    throw new IllegalArgumentException(String.format("Parameter must be <key=value>, but found <%s>", param));
                }
                params.put(parts[0], parts[1]);
            }
            else if (arg.equals("-c")) {
                if (++i >= args.length) {
                    throw new IllegalArgumentException("Config not specified for option -c");
                }
                String param =args[i];
                String[] parts = param.split("=", 2);
                if (parts.length != 2) {
                    throw new IllegalArgumentException(String.format("Config must be <key=value>, but found <%s>", param));
                }
                config.put(parts[0], parts[1]);
            }
            else if (arg.equals("-f")) {
                if (++i >= args.length) {
                    throw new IllegalArgumentException("Properties file not specified for option -f");
                }
                propertyFiles.add(args[i]);
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
