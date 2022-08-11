#!/bin/bash

[ ! $1 ] && echo "未配置 镜像名" && exit
[ ! $2 ] && echo "未配置 镜像版本" && exit

docker login -u flink  -p xxx harbor.xxx.cn/flink

docker build -t harbor.xxx.cn/flink/$1:$2 .

docker push harbor.xxx.cn/flink/$1:$2%