#!/bin/bash
stop-dfs.sh
stop-yarn.sh
stop-spark.sh
hdfs namenode -format
sudo systemctl stop sshd