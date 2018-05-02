# Ransomware in the Bitcoin Ecosystem | Dataset Extraction

This repository contains the ransomware seed dataset and expansion procedure
described in the paper Ransomware Payments in the Bitcoin Ecosystem (https://arxiv.org/abs/1804.04080). The
extraction procedure has been implemented of the open-souce [GraphSense
Cryptocurrency Analytics platform (v.0.3.1)](http://graphsense.info/).

## Seed address dataset

Collected ransomware seed addresses can be found in `data/seed_addresses.csv`

## Usage

Executing of this extraction job requires a running cluster with a deployment
of GraphSense and all pre-computed data.

Before running the job, replace pointers to SPARK-MASTER, at least two CASSANDRA
nodes and the job's target HDFS path in `./execute.sh` and `./src/main/scala/at/ac/ait/RansomwareDataset.scala`.

Run the job by running `./execute.sh`.

## Complete dataset 
The complete expanded dataset can also be downloaded at: https://zenodo.org/record/1238041#.WumE2dNuZR4 
