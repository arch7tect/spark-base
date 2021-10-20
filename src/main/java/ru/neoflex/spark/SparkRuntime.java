package ru.neoflex.spark;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

public class SparkRuntime {
    private Map<String, SparkJobBase> jobsRegistry = new HashMap<>();
    private Logger logger = LogManager.getLogger(SparkRuntime.class);
    private List<SparkJobBase> jobs = new ArrayList<>();

    public SparkRuntime registerJob(SparkJobBase job) {
        String name = job.getJobName();
        if (jobsRegistry.containsKey(name)) {
            throw new IllegalArgumentException(String.format("Job <%s> already registered", name));
        }
        jobsRegistry.put(name, job);
        return this;
    }

    private SparkSession.Builder initBuilder(SparkSession.Builder builder) {
        if (jobs.isEmpty() && jobsRegistry.size() == 1) {
            jobs.add(jobsRegistry.values().stream().findFirst().orElseThrow());
        }
        if (jobs.isEmpty()) {
            throw new IllegalArgumentException("Job to run not specified");
        }
        return builder.appName(jobs.get(0).getJobName());
    }

    public void run(String[] args) {
        try {
            registerSparkJobs();
            parseArgs(args);
            var spark = initBuilder(SparkSession.builder()).getOrCreate();
            try {
                for (var job: jobs) {
                    job.run(spark);
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
    }

    private void registerSparkJobs() throws InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        Package[] packages = Package.getPackages();
        for (Package p : packages) {
            SparkJobs annotation = p.getAnnotation(SparkJobs.class);
            if (annotation != null) {
                Class<?>[]  implementations = annotation.value();
                for (Class<?> impl : implementations) {
                    logger.info(impl.getSimpleName());
                    Object job = impl.getConstructor().newInstance();
                    if (!(job instanceof SparkJobBase)) {
                        throw new IllegalArgumentException(String.format("Class <%s> is not instance of SparkJobBase", impl.getSimpleName()));
                    }
                    registerJob((SparkJobBase) job);
                }
            }
        }
    }
}
