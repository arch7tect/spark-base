package ru.neoflex.spark.base;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Objects;

public abstract class SparkJobBase implements ISparkJob {
    private Logger logger;

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
            return Utils.readAsString(is);
        }
    }

    protected String formatResource(String path, Object... args) throws URISyntaxException, IOException {
        String format = getResource(path);
        return String.format(format, args);
    }

    protected String formatResource(String path, Map<String, String> params) throws URISyntaxException, IOException {
        String format = getResource(path);
        return Utils.replace(format, params);
    }
}
