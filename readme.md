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

# Debug (Windows)
Get driver host ip:
```shell
ipconfig | findstr IPv4
IPv4 Address. . . . . . . . . . . : 192.168.0.110
```
Run parameters:
```shell
-m spark://localhost:7077 -h -c spark.driver.host=192.168.0.110 -c spark.sql.catalogImplementation=hive -h -p file=test -p num=20 SimpleJob2
```
Winutils:
```shell
cd ~
git clone https://github.com/kontext-tech/winutils.git
```
Environments:
```shell
HADOOP_HOME=C:\Users\<user>\winutils\hadoop-3.2.0;PATH=C:\Users\<user>\winutils\hadoop-3.2.0\bin
```