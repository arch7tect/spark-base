package ru.neoflex.spark.base;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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

    private static String getTypeDescription(DataType dataType) {
        if (dataType instanceof ArrayType) {
            return String.format("ARRAY<%s>", getTypeDescription(((ArrayType) dataType).elementType()));
        }
        else if (dataType instanceof StructType) {
            String fields = Arrays.stream(((StructType) dataType).fields())
                    .map(field -> String.format("%s: %s", field.name(), getTypeDescription(field.dataType())))
                    .collect(Collectors.joining(", "));
            return String.format("STRUCT<%s>", fields);
        }
        else {
            return dataType.typeName();
        }
    }

    public static String createExternalTable(StructType schema, String name, String format, String location,
                                             String comment, Set<String> partitions, Map<String, String> options) {
        String fieldsStr = Arrays.stream(schema.fields())
                .filter(field -> partitions == null || partitions.stream().noneMatch(p->p.equalsIgnoreCase(field.name())))
                .map(field -> String.format("%s %s", field.name(), getTypeDescription(field.dataType())))
                .collect(Collectors.joining(", "));
        String commentStr = StringUtils.isBlank(comment) ? "" : String.format(" COMMENT \"%s\"", comment);
        String optionsStr = (options == null || options.isEmpty()) ? "" : String.format(" OPTIONS (%s)",
                options.entrySet().stream().map(e -> String.format("'%s': '%s'", e.getKey(), e.getValue()))
                .collect(Collectors.joining(", ")));
        String partitionsStr = partitions == null || partitions.size() == 0 ? "" : String.format(" PARTITIONED BY (%s)",
                Arrays.stream(schema.fields())
                .filter(field -> partitions.stream().anyMatch(p->p.equalsIgnoreCase(field.name())))
                .map(field -> String.format("%s %s", field.name(), getTypeDescription(field.dataType())))
                .collect(Collectors.joining(", ")));
        String formatStr = StringUtils.isBlank(format) ? "" : String.format(" STORED AS %s", format);
        String locationStr = StringUtils.isBlank(location) ? "" : String.format(" LOCATION '%s'", location);
        String createQuery = String.format("CREATE EXTERNAL TABLE %s(%s)%s%s%s%s%s",
                name, fieldsStr, commentStr, optionsStr, partitionsStr, formatStr, locationStr);
        return createQuery;
    }
}
