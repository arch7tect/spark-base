package ru.neoflex.spark.base;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
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

    protected String getResource(String path, Object... args) throws URISyntaxException, IOException {
        URL url = this.getClass().getClassLoader().getResource(path);
        Objects.requireNonNull(url, String.format("Resource <%s> not found", path));
        byte[] bytes = Files.readAllBytes(Paths.get(url.toURI()));
        return new String(bytes, StandardCharsets.UTF_8);
    }

    protected String formatResource(String path, Object... args) throws URISyntaxException, IOException {
        String format = getResource(path);
        return String.format(format, args);
    }
}
