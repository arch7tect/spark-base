package ru.neoflex.spark.annotations;

import com.google.auto.service.AutoService;
import org.stringtemplate.v4.*;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;

@SupportedSourceVersion(SourceVersion.RELEASE_8)
@AutoService(Processor.class)
public class SparkJobProcessor extends AbstractProcessor {
    @Override
    public Set<String> getSupportedAnnotationTypes() {
        Set<String> annotations = new LinkedHashSet<>();
        annotations.add(SparkJob.class.getCanonicalName());
        return annotations;
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        for (TypeElement annotation : annotations) {
            Set<? extends Element> annotatedElements = roundEnv.getElementsAnnotatedWith(annotation);
            Map<Boolean, List<Element>> annotatedMethods = annotatedElements.stream().collect(
                    Collectors.partitioningBy(element -> element.getKind() == ElementKind.CLASS));

            List<Element> classes = annotatedMethods.get(true);
            List<Element> others = annotatedMethods.get(false);
            others.forEach(element ->
                    processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
                            "@SparkJob must be applied to a class ", element));

            Map<String, List<Element>> registries = classes.stream().collect(
                    Collectors.groupingBy(element -> element.getAnnotation(SparkJob.class).registryClass()));
            for (Map.Entry<String, List<Element>> entry: registries.entrySet()) {
                String registryClass = entry.getKey();
                String packageName = null;
                int lastDot = registryClass.lastIndexOf('.');
                if (lastDot > 0) {
                    packageName = registryClass.substring(0, lastDot);
                }
                String simpleClassName = registryClass.substring(lastDot + 1);
                List<Map<String, String>> elements = entry.getValue().stream().
                        map(e-> new HashMap<String, String>(){{put("qualifiedName", ((TypeElement) e).getQualifiedName().toString());}}).
                        collect(Collectors.toList());
                try {
                    JavaFileObject classFile = processingEnv.getFiler().createSourceFile(registryClass);
                    try (PrintWriter out = new PrintWriter(classFile.openWriter())) {
                        generate(packageName, simpleClassName, elements, out);
                    }
                } catch (Throwable e) {
                    StringWriter writer = new StringWriter();
                    PrintWriter out = new PrintWriter(writer);
                    e.printStackTrace(out);
                    processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, writer.toString());
                }
            }


        }
        return true;
    }

    public void generate(String packageName, String simpleClassName, List<Map<String, String>> elements, PrintWriter out) {
        URL patternURL = this.getClass().getClassLoader().getResource("registry.stg");
        Objects.requireNonNull(patternURL, "registry.stg not found");
        STGroup regStg = new STGroupFile(patternURL);
        ST hronSt = regStg.getInstanceOf("aPackage");
        Objects.requireNonNull(hronSt, "rule <aPackage> not found");
        hronSt.add("packageName", packageName);
        hronSt.add("simpleClassName", simpleClassName);
        hronSt.add("elements", elements);
        STWriter wr = new AutoIndentWriter(out, "\n");
        wr.setLineWidth(STWriter.NO_WRAP);
        hronSt.write(wr, Locale.getDefault());
    }
}
