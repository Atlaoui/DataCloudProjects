#!/bin/bash
sudo systemctl start sshd
hdfs namenode -format
start-dfs.sh
start-yarn.sh
start-spark.sh
PATH_TO_SCRIPT="/home/adrien/SAR2_Assignments/DataCloud/DataGen/ressources_travaux_pratiques"
sh $PATH_TO_SCRIPT/check_start.sh hdfs
sh $PATH_TO_SCRIPT/check_start.sh yarn
sh $PATH_TO_SCRIPT/check_start.sh spark