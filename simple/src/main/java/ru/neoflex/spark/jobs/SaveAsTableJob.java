package ru.neoflex.spark.jobs;

import com.google.auto.service.AutoService;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import ru.neoflex.spark.base.ISparkJob;
import ru.neoflex.spark.base.SparkJobBase;
import ru.neoflex.spark.base.Utils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@AutoService(ISparkJob.class)
public class SaveAsTableJob extends SparkJobBase {

    @Override
    public void run(SparkSession spark, JavaSparkContext sc, Map<String, String> jobParameters) throws Exception {
        List<Integer> data = IntStream.range(0, 100).boxed().collect(Collectors.toList());
        spark.createDataset(data, Encoders.INT()).toDF("id").createTempView("is");
        String sql = formatResource("sql/addName.sql", jobParameters);
        Dataset<Row> dfSql = spark.sql(sql);
        String table = Utils.replace("${table:-test_table}", jobParameters);
        dfSql.write().mode(SaveMode.Overwrite).saveAsTable(table);
    }
}
