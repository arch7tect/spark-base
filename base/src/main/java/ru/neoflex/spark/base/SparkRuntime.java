package ru.neoflex.spark.base;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.json4s.jackson.Json;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class SparkRuntime {
    private static final Logger logger = LogManager.getLogger(SparkRuntime.class);
    private final List<ISparkJob> jobs = new ArrayList<>();
    private final List<String> propertyFiles = new ArrayList<>();
    private final Map<String, String> params = new HashMap<>();
    private final Map<String, String> config = new HashMap<>();
    private final Map<String, ISparkJob> registry = new HashMap<>();
    private String master;
    private Boolean hiveSupport = false;
    private final List<ObjectNode> workflows = new ArrayList<>();

    public SparkRuntime() {
        ServiceLoader<ISparkJob> loader = ServiceLoader.load(ISparkJob.class);
        for (ISparkJob job: loader) {
            registry.put(job.getJobName(), job);
        }
    }

    private SparkSession.Builder initBuilder(String appName, SparkSession.Builder builder) {
        builder.appName(appName);
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
            if (!workflows.isEmpty()) {
                for (ObjectNode wf: workflows) {
                    runWorkflow(wf);
                }
            }
            else {
                if (jobs.isEmpty()) {
                    if (registry.size() == 1) {
                        jobs.add(registry.values().stream().findFirst().get());
                    }
                }
                if (jobs.isEmpty()) {
                    throw new IllegalArgumentException("Job to run is not specified");
                }
                String appName = jobs.get(0).getJobName();
                runJobsInContext(appName, jobs);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void runWorkflow(ObjectNode wf) {
        List<JsonNode> tasks = StreamSupport.stream(wf.withArray("tasks").spliterator(), false)
                .collect(Collectors.toList());
        while (true) {
            Set<JsonNode> active = tasks.stream().filter(node->node.withArray("dependsOn").isEmpty())
                    .collect(Collectors.toSet());
            if (active.isEmpty()) break;
            List<JsonNode> newTasks = tasks.stream().filter(node->!active.contains(node)).collect(Collectors.toList());
            Consumer<String> result = (event)->{
                synchronized (newTasks) {
                    newTasks.forEach(t->{
                        List<String> events = StreamSupport.stream(t.withArray("dependsOn").spliterator(), false)
                                .map(JsonNode::asText)
                                .filter(s->!event.equals(s)).collect(Collectors.toList());
                        ArrayNode dependsOn = ((ObjectNode) t).putArray("dependsOn");
                        events.forEach(dependsOn::add);
                    });
                }
            };
            active.stream().map(node->startNode(node, result)).forEach(t-> {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
            tasks = newTasks;
        }
    }

    private Thread startNode(JsonNode node, Consumer<String> result) {
        String name = node.get("name").asText("<undefined>");
        Thread thread = new Thread(()->{
            try {
                logger.info(name + " starting");
                runNode(node);
                logger.info(name + " finished");
                result.accept(name);
            }
            catch (Throwable e) {
                String onError = node.get("onError").asText("<undefined>");
                logger.error(onError, e);
                result.accept(onError);
            }
        });
        thread.start();
        return thread;
    }

    private void runNode(JsonNode node) {
    }

    private void runJobsInContext(String appName, List<ISparkJob> jobs) throws Exception {
        SparkSession spark = initBuilder(appName, SparkSession.builder()).getOrCreate();
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
    }

    private void readParamsFromFiles(JavaSparkContext sc, Map<String, String> params) throws IOException {
        try (FileSystem fs = FileSystem.get(sc.hadoopConfiguration())) {
            for (String propertyFile: propertyFiles) {
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
            else if (arg.equals("-w")) {
                if (++i >= args.length) {
                    throw new IllegalArgumentException("Workflow file not specified for option -w");
                }
                String wfText = Utils.getResource(this.getClass().getClassLoader(), String.format("wf/%s.json", args[i]));
                workflows.add((ObjectNode) new JsonMapper().readTree(wfText));
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
