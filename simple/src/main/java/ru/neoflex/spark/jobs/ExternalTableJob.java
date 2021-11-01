package ru.neoflex.spark.jobs;

import com.google.auto.service.AutoService;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import ru.neoflex.spark.base.ISparkJob;
import ru.neoflex.spark.base.SparkJobBase;

import java.util.Map;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;

@AutoService(ISparkJob.class)
public class ExternalTableJob extends SparkJobBase {

    @Override
    public void run(SparkSession spark, JavaSparkContext sc, Map<String, String> jobParameters) throws Exception {
        spark.range(10000000).createOrReplaceTempView("t1");
        String sql1 = formatResource("sql/addName.sql", "tempTable", "t1");
        Dataset<Row> dfSql1 = sql(spark, sql1);
        saveAsExternalTable(spark, dfSql1, "names_1", "/data/names_1", null, null);

        spark.range(10000000).createOrReplaceTempView("t2");
        String sql2 = formatResource("sql/addName.sql", "tempTable", "t2");
        Dataset<Row> dfSql2 = sql(spark, sql2);
        saveAsExternalTable(spark, dfSql2, "names_2", "/data/names_2", null, null);

        Dataset<Row> df1 = spark.table("names_1").repartition(50, col("id"));
        Dataset<Row> df2 = spark.table("names_2").repartition(50, col("id"));

        Dataset<Row> dfJoined = df1.as("n1").join(df2.as("n2"), expr("n1.id=n2.id"), "inner").
                selectExpr("n1.id", "n1.uuid", "n2.name", "n2.description");
        saveAsExternalTable(spark, dfJoined, "names_joined_ext", "/data/names_joined", new String[] {"name"}, null);
        dropExternalTable(spark, "names_1");
        dropExternalTable(spark, "names_2");
    }
}
