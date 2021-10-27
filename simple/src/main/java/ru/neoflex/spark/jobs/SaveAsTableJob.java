package ru.neoflex.spark.jobs;

import com.google.auto.service.AutoService;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import ru.neoflex.spark.base.ISparkJob;
import ru.neoflex.spark.base.SparkJobBase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@AutoService(ISparkJob.class)
public class SaveAsTableJob extends SparkJobBase {

    protected Dataset<Row> createIDs(SparkSession spark, int from, int to) {
        List<Integer> data = IntStream.range(0, 100).boxed().collect(Collectors.toList());
        return spark.createDataset(data, Encoders.INT()).toDF("id");
    }

    @Override
    public void run(SparkSession spark, JavaSparkContext sc, Map<String, String> jobParameters) throws Exception {
        createIDs(spark, 0, 1000000).createOrReplaceTempView("t1");
        String sql1 = formatResource("sql/addName.sql", new HashMap<String, String>(){{put("tempTable", "t1");}});
        spark.sql(sql1).write().mode(SaveMode.Overwrite).bucketBy(100, "id").sortBy("id").saveAsTable("names_b1");
        Dataset<Row> df1 = spark.table("names_b1");
        createIDs(spark, 0, 100000).createOrReplaceTempView("t2");
        String sql2 = formatResource("sql/addName.sql", new HashMap<String, String>(){{put("tempTable", "t2");}});
        spark.sql(sql2).write().mode(SaveMode.Overwrite).bucketBy(100, "id").sortBy("id").saveAsTable("names_b2");
        Dataset<Row> df2 = spark.table("names_b2");
        Dataset<Row> dfJoined = df1.join(df2, df1.col("id").$eq$eq$eq(df2.col("id")), "inner").
                selectExpr("names_b1.id", "names_b1.uuid", "names_b2.name", "names_b2.description");
        dfJoined.write().mode(SaveMode.Overwrite).saveAsTable("names_joined");
    }
}
