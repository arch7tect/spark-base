```shell
mvn clean install
spark-submit simple/target/simple-1.0-SNAPSHOT-shaded.jar -m local[*] -p file=test.json SimpleJob2
```