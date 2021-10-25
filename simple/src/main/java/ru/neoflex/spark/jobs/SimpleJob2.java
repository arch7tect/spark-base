package ru.neoflex.spark.jobs;

import com.google.auto.service.AutoService;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import ru.neoflex.spark.base.ISparkJob;
import ru.neoflex.spark.base.SparkJobBase;
import ru.neoflex.spark.base.Utils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@AutoService(ISparkJob.class)
public class SimpleJob2 extends SparkJobBase {

    @Override
    public void run(SparkSession spark, JavaSparkContext sc, Map<String, String> jobParameters) throws Exception {
        List<Integer> data = IntStream.range(0, 100).boxed().collect(Collectors.toList());
        spark.createDataset(data, Encoders.INT()).coalesce(3).toDF("id").createTempView("is");
        String sql = formatResource("sql/addName.sql", jobParameters);
        Dataset<Row> dfSql = spark.sql(sql);
        dfSql.persist(StorageLevel.MEMORY_AND_DISK());
        dfSql.show();
        String path = Utils.replace("/data/${file:-simple.parquet}", jobParameters);
        dfSql.write().partitionBy("name").mode(SaveMode.Overwrite).parquet(path);
        createTable(spark, dfSql.schema(), "SIMPLE", "PARQUET", path, "Simple table",
                Arrays.stream(new String[] {"name"}).collect(Collectors.toSet()), null);
    }
}
