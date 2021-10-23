package ru.neoflex.spark.base;

import org.apache.commons.text.StringSubstitutor;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;

public abstract class SparkJobBase implements ISparkJob {
    private Logger logger = LogManager.getLogger(SparkRuntime.class);

    public String getJobName() {
        return this.getClass().getSimpleName();
    }

    protected Logger getLogger() {
        if (logger == null) {
            logger = LogManager.getLogger(this.getClass());
        }
        return logger;
    }

    protected void info(String format, Object... args) {
        getLogger().info(String.format(format, args));
    }

    protected String getResource(String path) throws IOException {
        try (InputStream is = this.getClass().getClassLoader().getResourceAsStream(path)) {
            Objects.requireNonNull(is, String.format("Resource <%s> not found", path));
            return readAsString(is);
        }
    }

    private static String readAsString(InputStream is) throws IOException {
        Reader in = new InputStreamReader(is, StandardCharsets.UTF_8);
        char[] buffer = new char[1024];
        StringBuilder out = new StringBuilder();
        for (int numRead; (numRead = in.read(buffer, 0, buffer.length)) > 0; ) {
            out.append(buffer, 0, numRead);
        }
        return out.toString();
    }

    protected String formatResource(String path, Object... args) throws URISyntaxException, IOException {
        String format = getResource(path);
        return String.format(format, args);
    }

    protected String formatResource(String path, Map<String, String> params) throws URISyntaxException, IOException {
        String format = getResource(path);
        return substitute(format, params);
    }

    protected String substitute(String text, Map<String, String> params) {
        StringSubstitutor ss = new StringSubstitutor(params);
        ss.setEnableSubstitutionInVariables(true);
        ss.setEnableUndefinedVariableException(true);
        return ss.replace(text);
    }
}
