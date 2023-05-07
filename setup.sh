#!/bin/sh

# Compile Spark classes
sbt compile
sbt package
mv target/scala-2.12/spark_transformations.jar jars/spark_transformations.jar

# Download postgresql driver
if [ ! -f ./jars/postgresql-42.6.0.jar ]
then
    echo "No PostgreSQL driver found in ./jars/ folder. Downloading..."
    curl -o jars/postgresql-42.6.0.jar https://jdbc.postgresql.org/download/postgresql-42.6.0.jar
    echo "PostgreSQL driver downloaded."
else
    echo "PostgreSQL driver found."
fi