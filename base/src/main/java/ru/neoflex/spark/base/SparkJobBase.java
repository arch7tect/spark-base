package ru.neoflex.spark.base;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class SparkJobBase implements ISparkJob {
    private transient Logger logger;

    public String getJobName() {
        return this.getClass().getSimpleName();
    }

    protected Logger getLogger() {
        if (logger == null) {
            logger = LogManager.getLogger(this.getClass());
        }
        return logger;
    }

    protected void info(String format, Object... args) {
        getLogger().info(String.format(format, args));
    }

    protected String getResource(String path) throws IOException {
        try (InputStream is = this.getClass().getClassLoader().getResourceAsStream(path)) {
            Objects.requireNonNull(is, String.format("Resource <%s> not found", path));
            return Utils.readAsString(is);
        }
    }

    protected String formatResource(String path, Object... args) throws IOException {
        if(args.length % 2 == 1)
            throw new IllegalArgumentException("Args length must be even");
        Map<String, String> params =  IntStream.range(0, args.length/2).map(i -> i*2)
                .collect(HashMap::new, (m, i) -> m.put(args[i].toString(), args[i + 1].toString()), Map::putAll);        String format = getResource(path);
        return formatResource(path, params);
    }

    protected String formatResource(String path, Map<String, String> params) throws IOException {
        String format = getResource(path);
        return Utils.replace(format, params);
    }

    public void createEternalTable(SparkSession spark, StructType schema, String name, String format, String location,
                                   String comment, String[] partitions, Map<String, String> options) {
        spark.sql(String.format("DROP TABLE IF EXISTS %s", name));
        String createQuery = Utils.createExternalTable(schema, name, format, location, comment,
                partitions == null ? null : Arrays.stream(partitions).collect(Collectors.toSet()),
                options);
        info("Creating table:\n%s", createQuery);
        spark.sql(createQuery);
        if (partitions != null && partitions.length > 0) {
            spark.sql(String.format("MSCK REPAIR TABLE %s", name));
        }
    }

    public void saveAsExternalTable(SparkSession spark, Dataset<Row> df, String name, String location,
                                    String[] partitions, String comment) {
        writeToExternalTable(spark, df, SaveMode.Overwrite, name, location, partitions, comment);
    }

    public void appendToExternalTable(SparkSession spark, Dataset<Row> df, String name, String location,
                                      String[] partitions, String comment) {
        writeToExternalTable(spark, df, SaveMode.Append, name, location, partitions, comment);
    }

    public void writeToExternalTable(SparkSession spark, Dataset<Row> df, SaveMode saveMode, String name, String location,
                                     String[] partitions, String comment) {
        DataFrameWriter<Row> dfw = df.write().mode(saveMode);
        if (partitions != null && partitions.length > 0) {
            dfw = dfw.partitionBy(partitions);
        }
        dfw.parquet(location);
        createEternalTable(spark, df.schema(), name, "PARQUET", location, comment, partitions, null);
    }
}
