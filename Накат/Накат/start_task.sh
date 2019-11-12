#!/usr/bin/env bash

#всегда пишем 2 строки ниже - экспортируем адрес oozie
OOZIE_URL="http://hd-has015.vimpelcom.ru:11000/oozie"
export OOZIE_URL

PROJECT_NAME=Cdr_DimBan_Subscriber_Cnt         #название задачи
HDFS_OUTPUT_PATH=/user/pshevlyakova/nalukin/oozie/mapReduce/${PROJECT_NAME}   #путь к папке с задачей в hdfs
WORKING_DIR=${HDFS_OUTPUT_PATH}/work		   #рабочая директория - там будут все файлы задачи

SHEME=arstel.cdr_dim_ban_nalukin                          #название схемы. У нас есть доступ на запись только в схему arstel. К названию таблицы обычно добавляем свой префикс

cp /etc/hive/conf/hive-site.xml ${PWD}		   #копируем файл с настройками из общей папки

hadoop fs -rm -R ${WORKING_DIR}		           #удаление старой папки в hdfs и копирование новой с аппноды
hadoop fs -mkdir -p ${WORKING_DIR}
hadoop fs -put * ${WORKING_DIR}

#При необходимости можно запустить скрипт создания таблиц
#echo "START"
#hive \
#-hivevar SCHEME=${SCHEME} \
#-f hive/create_tables.sql;

oozie job -auth KERBEROS \
-config job.properties \
-D WORKING_DIR=${WORKING_DIR} \
-D PROJECT_NAME=${PROJECT_NAME} \
-run