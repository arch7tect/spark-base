package ru.neoflex.spark.tests;

import org.junit.Test;
import ru.neoflex.spark.annotations.SparkJobProcessor;

import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GenTest {
    @Test
    public void doTest () {
        SparkJobProcessor processor = new SparkJobProcessor();
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);

        processor.generate("ru.neoflex.spark.jobs", "Registry",
                new ArrayList<Map<String, String>>() {{add(new HashMap<String, String>(){{put("qualifiedName", "ru.neoflex.spark.jobs.SimpleJob");}});}}, pw);
        String code = sw.toString();
    }
}
