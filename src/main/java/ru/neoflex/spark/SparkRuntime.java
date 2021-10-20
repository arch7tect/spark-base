package ru.neoflex.spark;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class SparkRuntime {
    private Map<String, SparkJobBase> jobs = new HashMap<>();
    private Logger logger = LogManager.getLogger(SparkRuntime.class);

    public SparkRuntime registerJob(SparkJobBase job) {
        String name = job.getJobName();
        if (jobs.containsKey(name)) {
            throw new IllegalArgumentException(String.format("Job <%s> already registered", name));
        }
        jobs.put(name, job);
        return this;
    }

    public void run(String[] args) {
        try {
            registerSparkJobs();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
