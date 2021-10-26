## Requirements (Windows)
- Docker desktop
- git
- mvn


## Run
```shell
git clone https://github.com/arch7tect/spark-base.git
cd spark-base
docker-compose -f docker/docker-compose.yml pull
docker-compose -f docker/docker-compose.yml up -d
mvn clean install
cp ./simple/target/simple-1.0-SNAPSHOT-shaded.jar ./docker/data
docker-compose -f docker/docker-compose.yml exec spark sh
cd /data
spark-submit simple-1.0-SNAPSHOT-shaded.jar -h -p file=test -p num=20 SimpleJob2
```

## URLS
Resource|URL
------|---
Spark Master UI|http://localhost:8080/
Spark Master|spark://localhost:7077
Thrift Server UI|http://localhost:4040/
Thrift Server|jdbc:hive2://localhost:10000
Spark History Server|http://localhost:18080/
Livy UI|http://localhost:8998/
Hue|http://localhost:8888/

# Debug (Idea)
Edit Run/Debug configuration->Application:
```shell
Run on: docker
Image Tag: openjdk:8-jdk-slim
Run Options: --rm --network=docker_spark_net  --volume=C:\Users\<User>\github\spark-base\docker\data:/data
Build&Run: java8 -cp simple ru.neoflex.spark.base.Main 
(Add dependencies with the 'provided' scope to classpath)
Args: -m spark://spark-master:7077 -h -c spark.hive.metastore.uris=thrift://metastore:9083 -c spark.sql.warehouse.dir=file:/data/warehouse -p file=test -p num=20 SimpleJob2
```
