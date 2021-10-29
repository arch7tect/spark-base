package ru.neoflex.spark.jobs;

import com.google.auto.service.AutoService;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import ru.neoflex.spark.base.ISparkJob;
import ru.neoflex.spark.base.SparkJobBase;

import java.util.Map;

@AutoService(ISparkJob.class)
public class SaveAsTableJob extends SparkJobBase {

    @Override
    public void run(SparkSession spark, JavaSparkContext sc, Map<String, String> jobParameters) throws Exception {
        info("bucketingEnabled: %b", spark.sessionState().conf().bucketingEnabled());
        //spark.conf().set("spark.sql.autoBroadcastJoinThreshold", -1);

        spark.range(5000000).createOrReplaceTempView("t1");
        String sql1 = formatResource("sql/addName.sql", "tempTable", "t1");
        spark.sql(sql1).orderBy("uuid").write().mode(SaveMode.Overwrite)
                .bucketBy(100, "id").sortBy("id").saveAsTable("names_b1");
        Dataset<Row> df1 = spark.table("names_b1");

        spark.range(5000000).createOrReplaceTempView("t2");
        String sql2 = formatResource("sql/addName.sql", "tempTable", "t2");
        spark.sql(sql2).orderBy("uuid").write().mode(SaveMode.Overwrite)
                .bucketBy(100, "id").sortBy("id").saveAsTable("names_b2");
        Dataset<Row> df2 = spark.table("names_b2");

        Dataset<Row> dfJoined = df1.join(df2, df1.col("id").$eq$eq$eq(df2.col("id")), "inner").
                selectExpr("names_b1.id", "names_b1.uuid", "names_b2.name", "names_b2.description");
        dfJoined.write().mode(SaveMode.Overwrite).saveAsTable("names_joined");
    }
}
