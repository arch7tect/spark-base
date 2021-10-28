package ru.neoflex.spark.jobs;

import com.google.auto.service.AutoService;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF0;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import ru.neoflex.spark.base.ISparkJob;
import ru.neoflex.spark.base.SparkJobBase;

import java.io.Serializable;
import java.util.Map;

import static org.apache.spark.sql.functions.udf;
@AutoService(ISparkJob.class)
public class UdfJob extends SparkJobBase implements Serializable {

    @Override
    public void run(SparkSession spark, JavaSparkContext sc, Map<String, String> jobParameters) throws Exception {
        UserDefinedFunction r = udf((UDF0<Double>) () -> Math.random(), DataTypes.DoubleType);
        r.asNondeterministic();
        spark.udf().register("r", r);

        spark.range(5000).selectExpr("id", "uuid() as uuid", "r() as r")
                .write().mode(SaveMode.Overwrite).saveAsTable("random");
    }
}
