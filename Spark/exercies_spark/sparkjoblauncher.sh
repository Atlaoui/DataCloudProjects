#!/bin/bash
if [ $# -lt 2 ]
then
    echo "usage : <java_project_root> <main_class> <arg1> <arg2> ..."
    exit 1
fi

project_root=$1
main_class=$2
file_jar=/tmp/spark_prog.jar

shift 2
jar cvf $file_jar -C $project_root .
spark-submit --master spark://127.0.0.1:7077 --class $main_class
$file_jar $@