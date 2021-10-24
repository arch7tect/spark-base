package ru.neoflex.spark.base;

import org.apache.commons.text.StringSubstitutor;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class Utils {
    public static String readAsString(InputStream is) throws IOException {
        Reader in = new InputStreamReader(is, StandardCharsets.UTF_8);
        char[] buffer = new char[1024];
        StringBuilder out = new StringBuilder();
        for (int numRead; (numRead = in.read(buffer, 0, buffer.length)) > 0; ) {
            out.append(buffer, 0, numRead);
        }
        return out.toString();
    }

    public static String replace(String text, Map<String, String> params) {
        StringSubstitutor ss = new StringSubstitutor(params);
        ss.setEnableSubstitutionInVariables(true);
        ss.setEnableUndefinedVariableException(true);
        return ss.replace(text);
    }
}
