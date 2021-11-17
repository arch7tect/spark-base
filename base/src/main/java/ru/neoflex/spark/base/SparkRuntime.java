package ru.neoflex.spark.base;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class SparkRuntime {
    private static final Logger logger = LogManager.getLogger(SparkRuntime.class);
    private final List<ISparkJob> jobs = new ArrayList<>();
    private final List<String> propertyFiles = new ArrayList<>();
    private final Map<String, String> params = new HashMap<>();
    private final Map<String, String> config = new HashMap<>();
    private final Map<String, ISparkJob> registry = new HashMap<>();
    private final Map<String, Map<String, Object>> workflows = new HashMap<>();
    private String master;
    private Boolean hiveSupport = false;

    public SparkRuntime() {
        ServiceLoader<ISparkJob> loader = ServiceLoader.load(ISparkJob.class);
        for (ISparkJob job : loader) {
            registry.put(job.getJobName(), job);
        }
    }

    private SparkSession.Builder initBuilder(String appName, SparkSession.Builder builder) {
        builder.appName(appName);
        if (master != null) {
            builder.master(master);
        }
        for (String key : config.keySet()) {
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
            if (!workflows.isEmpty()) {
                for (Map.Entry<String, Map<String, Object>> wf: workflows.entrySet()) {
                    String appName = wf.getKey();
                    executeInContext(appName, (name, spark, sc, jobParameters) -> {
                        runWorkflow(name, wf.getValue(), spark, sc, jobParameters);
                    });
                }
            } else {
                if (jobs.isEmpty()) {
                    if (registry.size() == 1) {
                        jobs.add(registry.values().stream().findFirst().get());
                    }
                }
                if (jobs.isEmpty()) {
                    throw new IllegalArgumentException("Job to run is not specified");
                }
                for (ISparkJob job : jobs) {
                    String appName = job.getJobName();
                    logger.info(String.format("Running job <%s>", job.getJobName()));
                    executeInContext(appName, (name, spark, sc, jobParameters) -> {
                        runJob(name, job, spark, sc, jobParameters);
                    });
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void runJob(String name, ISparkJob job, SparkSession spark, JavaSparkContext sc, Map<String, String> jobParameters) throws Exception {
        job.run(name, spark, sc, sc.broadcast(jobParameters).getValue());
    }

    private Map<String, String> getWFParams(Map<String, Object> wf) {
        Map<String, String> wfParameters = (Map<String, String>) wf.getOrDefault("params", new HashMap<>());
        List<Map<String, Object>> tasks = (List<Map<String, Object>>) wf.getOrDefault("tasks", Collections.emptyList());
        for (Map<String, Object> task: tasks) {
            String name = task.getOrDefault("name", "<undefined>").toString();
            Map<String, String> taskParameters = (Map<String, String>) wf.getOrDefault("params", new HashMap<>());
            for (String key: taskParameters.keySet()) {
                wfParameters.put(name + "." + key, taskParameters.get(key));
            }
        }
        return wfParameters;
    }

    private void runWorkflow(String name, Map<String, Object> wf, SparkSession spark, JavaSparkContext sc, Map<String, String> jobParameters) throws InterruptedException {
        Map<String, String> wfParameters = (Map<String, String>) wf.getOrDefault("params", new HashMap<>());
        wfParameters.putAll(jobParameters);
        logger.info(String.format("Running workflow <%s>", name));
        List<Map<String, Object>> tasks = (List<Map<String, Object>>) wf.getOrDefault("tasks", Collections.emptyList());
        tasks.forEach(t -> t.put("dependsSet", ((List<String>) t.getOrDefault("dependsOn", Collections.emptyList()))
                .stream().collect(Collectors.toSet())));
        ExecutorService service = Executors.newCachedThreadPool();
        reschedule(name, service, tasks, null, spark, sc, wfParameters);
        do {
            logger.info(String.format("Waiting for workflow <%s> termination", name));
        } while (service.awaitTermination(5, TimeUnit.SECONDS));
    }

    private boolean reschedule(String name, ExecutorService service, List<Map<String, Object>> tasks, String event,
                               SparkSession spark, JavaSparkContext sc, Map<String, String> jobParameters) {
        synchronized (tasks) {
            if (event != null) {
                tasks.forEach(t -> ((Set<String>) t.get("dependsSet")).remove(event));
            }
            Set<Map<String, Object>> schedule = tasks.stream()
                    .filter(t -> ((Set<String>) t.get("dependsSet")).isEmpty())
                    .collect(Collectors.toSet());
            tasks.removeAll(schedule);
            schedule.forEach(t -> service.submit(() -> {
                try {
                    String taskName = t.getOrDefault("name", "<undefined>").toString();
                    logger.info(name + "." + taskName + " starting");
                    String type = t.getOrDefault("type", "<undefined>").toString();
                    Map<String, Object> config = (Map<String, Object>) t.getOrDefault("config", new HashMap<>());
                    if ("Dummy".equals(type)) {
                        // Dummy
                    }
                    else if ("Exit".equals(type)) {
                        synchronized (tasks) { tasks.clear(); }
                        service.shutdown();
                    }
                    else if ("Kill".equals(type)) {
                        String message = config.getOrDefault("message", "Kill").toString();
                        logger.error(message);
                        synchronized (tasks) { tasks.clear(); }
                        service.shutdownNow();
                    }
                    else if ("SparkJob".equals(type)) {
                        runSparkJobTask(name, spark, sc, jobParameters, config);
                    }
                    else if ("Workflow".equals(type)) {
                    }
                    else {
                        logger.error(String.format("Unknown node type <%s> for node <%s>", type, name));
                        synchronized (tasks) { tasks.clear(); }
                        service.shutdownNow();
                    }
                    logger.info(name + "." + taskName + " finished");
                    reschedule(name, service, tasks, taskName, spark, sc, jobParameters);
                } catch (Throwable e) {
                    String onError = t.getOrDefault("onError", "<undefined>").toString();
                    logger.error(onError, e);
                    if (!reschedule(name, service, tasks, onError, spark, sc, jobParameters)) {
                        synchronized (tasks) { tasks.clear(); }
                        service.shutdownNow();
                    }
                }
            }));
            return !schedule.isEmpty();
        }
    }

    private void runSparkJobTask(String name, SparkSession spark, JavaSparkContext sc, Map<String, String> jobParameters, Map<String, Object> config) throws Exception {
        String jobName = config.getOrDefault("jobName", "<undefined>").toString();
        ISparkJob job = registry.get(jobName);
        if (job == null) {
            throw new IllegalArgumentException(String.format("Job not found <%s>", jobName));
        }
        Map<String, String> params = (Map<String, String>) config.getOrDefault("params", new HashMap<>());
        params.putAll(jobParameters);
        job.run(name, spark, sc, sc.broadcast(params).getValue());
    }

    private void runNode(String s, Map<String, Object> t, String name, SparkSession spark, JavaSparkContext sc, Map<String, String> jobParameters) {
    }

    private void executeInContext(String appName, ISparkExecutable executable) throws Exception {
        SparkSession spark = initBuilder(appName, SparkSession.builder()).getOrCreate();
        try {
            JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
            Map<String, String> paramsEffective = new HashMap<>();
            if (!propertyFiles.isEmpty()) {
                readParamsFromFiles(sc, paramsEffective);
            }
            paramsEffective.putAll(params);
            executable.run(appName, spark, sc, paramsEffective);
        } finally {
            spark.stop();
        }
    }

    private void readParamsFromFiles(JavaSparkContext sc, Map<String, String> params) throws IOException {
        try (FileSystem fs = FileSystem.get(sc.hadoopConfiguration())) {
            for (String propertyFile : propertyFiles) {
                logger.info(String.format("Load parameters from file <%s>", propertyFile));
                try (FSDataInputStream is = fs.open(new Path(propertyFile))) {
                    Properties props = new Properties();
                    props.load(is);
                    props.forEach((key, value) -> params.put(key.toString(), value.toString()));
                }
            }
        }
    }

    private void parseArgs(String[] args) throws JsonProcessingException {
        for (int i = 0; i < args.length; ++i) {
            String arg = args[i];
            if (arg.equals("-m")) {
                if (++i >= args.length) {
                    throw new IllegalArgumentException("Master not specified for option -m");
                }
                master = args[i];
            } else if (arg.equals("-h")) {
                hiveSupport = !hiveSupport;
            } else if (arg.equals("-p")) {
                if (++i >= args.length) {
                    throw new IllegalArgumentException("Parameter not specified for option -p");
                }
                String param = args[i];
                String[] parts = param.split("=", 2);
                if (parts.length != 2) {
                    throw new IllegalArgumentException(String.format("Parameter must be <key=value>, but found <%s>", param));
                }
                params.put(parts[0], parts[1]);
            } else if (arg.equals("-c")) {
                if (++i >= args.length) {
                    throw new IllegalArgumentException("Config not specified for option -c");
                }
                String param = args[i];
                String[] parts = param.split("=", 2);
                if (parts.length != 2) {
                    throw new IllegalArgumentException(String.format("Config must be <key=value>, but found <%s>", param));
                }
                config.put(parts[0], parts[1]);
            } else if (arg.equals("-f")) {
                if (++i >= args.length) {
                    throw new IllegalArgumentException("Properties file not specified for option -f");
                }
                propertyFiles.add(args[i]);
            } else if (arg.equals("-w")) {
                if (++i >= args.length) {
                    throw new IllegalArgumentException("Workflow file not specified for option -w");
                }
                String wfText = Utils.getResource(this.getClass().getClassLoader(), String.format("wf/%s.json", args[i]));
                workflows.put(args[i], new JsonMapper().readValue(wfText, Map.class));
            } else {
                ISparkJob job = registry.get(arg);
                if (job == null) {
                    throw new IllegalArgumentException(String.format("Job <%s> not found", arg));
                }
                jobs.add(job);
            }
        }
    }
}
