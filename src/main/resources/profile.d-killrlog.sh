#!/usr/bin/env bash

export SPARK_MASTER="spark://10.240.0.2:7077"
export CASSANDRA_CONTACT_POINTS="10.240.0.2"
export kl_executor_memory="2g"
export kl_cores="5"
export kl_brokers="10.240.0.2:9092,10.240.0.3:9092,10.240.0.4:9092"

