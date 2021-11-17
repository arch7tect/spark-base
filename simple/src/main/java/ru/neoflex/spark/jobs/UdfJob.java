package ru.neoflex.spark.jobs;

import com.google.auto.service.AutoService;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF0;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.util.ShutdownHookManager;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import ru.neoflex.spark.base.ISparkJob;
import ru.neoflex.spark.base.SparkJobBase;
import scala.runtime.BoxedUnit;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import static org.apache.spark.sql.functions.udf;

@AutoService(ISparkJob.class)
public class UdfJob extends SparkJobBase {

    public static class ExchangeRateUDF implements UDF2<Date, String, BigDecimal> {
        private static final SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
        private static final Logger logger = LogManager.getLogger(ExchangeRateUDF.class);
        private static CloseableHttpClient client;

        @Override
        public BigDecimal call(Date date, String code) throws Exception {
            initClient();
            String uri = "https://www.cbr.ru/scripts/XML_daily.asp?date_req=" + dateFormat.format(date);
            logger.debug(code + "<-" + uri);
            try (CloseableHttpResponse response = client.execute(new HttpGet(uri))) {
                if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                    String body = EntityUtils.toString(response.getEntity());
                    Document document = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(
                            new InputSource(new StringReader(body)));
                    NodeList elements = document.getElementsByTagName("Valute");
                    for (int i = 0; i < elements.getLength(); ++i) {
                        Element element = (Element) elements.item(i);
                        String charCode = element.getElementsByTagName("CharCode").item(0).getTextContent();
                        if (code.equals(charCode)) {
                            String value = element.getElementsByTagName("Value").item(0).getTextContent();
                            return new BigDecimal(value.replace(",", "."));
                        }
                    }
                }
            }
            return BigDecimal.ZERO;
        }

        private void initClient() {
            if (client == null) {
                logger.info("Create HttpClient");
                client = HttpClientBuilder.create()
                        .setConnectionManager(new PoolingHttpClientConnectionManager())
                        .build();
                ShutdownHookManager.addShutdownHook(() -> {
                    logger.info("Close HttpClient from ShutdownHookManager");
                    try {
                        if (client != null) {
                            client.close();
                            client = null;
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return BoxedUnit.UNIT;
                });
            }
        }
    }

    @Override
    public void run(String name, SparkSession spark, JavaSparkContext sc, Map<String, String> jobParameters) throws Exception {
        spark.udf().register("r", udf((UDF0<Double>) Math::random, DataTypes.DoubleType)
                .asNondeterministic());
        spark.udf().register("rate", udf(new ExchangeRateUDF(), DataTypes.createDecimalType(18, 8)));

        info("Test rate(): ${rate}",
                "rate", new ExchangeRateUDF().call(new Date(), "GBP").toString());

        spark
                .range(500)
                .selectExpr("date_sub(current_date(), cast(id as int)) as date", "'GBP' as code")
                .selectExpr("date", "code",
                        "rate(date, code) as rate",
                        "r() as r"
                ).write().mode(SaveMode.Overwrite).saveAsTable("random");
    }
}
