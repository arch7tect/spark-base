package ru.neoflex.spark.base;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;

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
        getLogger().info(Utils.replace(format, args));
    }

    protected void info(String format, Map<String, String> params, Object... args) {
        getLogger().info(Utils.replace(format, params, args));
    }

    protected String getResource(String path) throws IOException {
        try (InputStream is = this.getClass().getClassLoader().getResourceAsStream(path)) {
            Objects.requireNonNull(is, String.format("Resource <%s> not found", path));
            return Utils.readAsString(is);
        }
    }

    protected String formatResource(String path, Object... args) throws IOException {
        return formatResource(path, Utils.getParamsFromArgs(args));
    }

    protected String formatResource(String path, Map<String, String> params) throws IOException {
        String format = getResource(path);
        return Utils.replace(format, params);
    }

    protected String formatSQL(String name, Map<String, String> params) throws IOException {
        String path = String.format("sql/%s/%s.sql", getJobName(), name);
        return formatResource(path, params);
    }

    protected String formatSQL(String name, Object... args) throws IOException {
        return formatSQL(name, Utils.getParamsFromArgs(args));
    }

    protected Dataset<Row> sql(SparkSession spark, String sql) {
        info("SQL: ${sql}", "sql", sql);
        return spark.sql(sql);
    }

    protected void createEternalTable(SparkSession spark, StructType schema, String name, String format, String location,
                                   String comment, String[] partitions, Map<String, String> options) {
        sql(spark, String.format("DROP TABLE IF EXISTS %s", name));
        String createQuery = Utils.createExternalTable(schema, name, format, location, comment,
                partitions, options);
        sql(spark, createQuery);
        if (partitions != null && partitions.length > 0) {
            sql(spark, String.format("MSCK REPAIR TABLE %s", name));
        }
    }

    protected void saveAsExternalTable(SparkSession spark, Dataset<Row> df, String name, String location,
                                    String[] partitions, String comment) {
        writeToExternalTable(spark, df, SaveMode.Overwrite, name, location, partitions, comment);
    }

    protected void appendToExternalTable(SparkSession spark, Dataset<Row> df, String name, String location,
                                      String[] partitions, String comment) {
        writeToExternalTable(spark, df, SaveMode.Append, name, location, partitions, comment);
    }

    protected void writeToExternalTable(SparkSession spark, Dataset<Row> df, SaveMode saveMode, String name, String location,
                                     String[] partitions, String comment) {
        DataFrameWriter<Row> dfw = df.write().mode(saveMode);
        if (partitions != null && partitions.length > 0) {
            dfw = dfw.partitionBy(partitions);
        }
        dfw.parquet(location);
        createEternalTable(spark, df.schema(), name, "PARQUET", location, comment, partitions, null);
    }

    protected void dropExternalTable(SparkSession spark, String name) throws IOException {
        List<Row> rows = sql(spark, String.format("DESCRIBE TABLE EXTENDED %s", name)).collectAsList();
        Map<String, String> props = rows.stream().collect(Collectors.toMap(r -> r.getString(0), r -> r.getString(1)));
        boolean external = "EXTERNAL".equals(props.get("Type"));
        String location = props.get("Location");
        sql(spark, String.format("DROP TABLE %s", name));
        if (external && !StringUtils.isBlank(location)) {
            info("Deleting ${location}", "location", location);
            FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
            fs.delete(new Path(location), true);
        }
    }
}
