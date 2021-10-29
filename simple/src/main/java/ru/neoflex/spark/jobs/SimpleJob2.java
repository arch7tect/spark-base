package ru.neoflex.spark.jobs;

import com.google.auto.service.AutoService;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import ru.neoflex.spark.base.ISparkJob;
import ru.neoflex.spark.base.SparkJobBase;
import ru.neoflex.spark.base.Utils;

import java.util.Map;

@AutoService(ISparkJob.class)
public class SimpleJob2 extends SparkJobBase {

    @Override
    public void run(SparkSession spark, JavaSparkContext sc, Map<String, String> jobParameters) throws Exception {
        spark.range(100).coalesce(3).createOrReplaceTempView("is");
        String sql = formatResource("sql/addName.sql", jobParameters);
        Dataset<Row> dfSql = spark.sql(sql);
        dfSql.persist(StorageLevel.MEMORY_AND_DISK());
        dfSql.show();
        String path = Utils.replace("/data/${file:-simple.parquet}", jobParameters);
        String[] partitions = new String[] {"name"};
        dfSql.write().partitionBy(partitions).mode(SaveMode.Overwrite).parquet(path);
        createTable(spark, dfSql.schema(), "SIMPLE", "PARQUET", path, "Simple table",
                partitions, null);
    }
}
