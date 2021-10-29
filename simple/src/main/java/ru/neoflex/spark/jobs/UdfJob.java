package ru.neoflex.spark.jobs;

import com.google.auto.service.AutoService;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF0;
import org.apache.spark.sql.types.DataTypes;
import ru.neoflex.spark.base.ISparkJob;
import ru.neoflex.spark.base.SparkJobBase;

import java.util.Map;

import static org.apache.spark.sql.functions.udf;
@AutoService(ISparkJob.class)
public class UdfJob extends SparkJobBase {

    @Override
    public void run(SparkSession spark, JavaSparkContext sc, Map<String, String> jobParameters) throws Exception {
        spark.udf().register("r", udf((UDF0<Double>) Math::random, DataTypes.DoubleType)
                .asNondeterministic());

        spark.range(5000).selectExpr("id", "uuid() as uuid", "r() as r")
                .write().mode(SaveMode.Overwrite).saveAsTable("random");
    }
}
