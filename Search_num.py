#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import shutil
# Path for spark source folder
os.environ['SPARK_HOME'] = "/opt/mcb/hadoop/spark-2.1.1"

# Append pyspark to Python Path
sys.path.append("/opt/mcb/hadoop/spark-2.1.1/python")
from pyspark import SparkContext,SparkConf,StorageLevel

def check_error_num(line):
    '''
    遍历文件每一行，返回失败交易的手机号码
    :return:
    '''
    lines = line.split('|')
    if lines[28] != '0000':
        return lines[14]

def check_num_dict(line):
    '''
    遍历文件每一行，返回dict{ id_value:(req_channl,req_date_time,upay_biz_seq,crm_rsp_code)}
    :param line:
    :return:
    '''
    lines =line.split('|')
    tup=(lines[2],lines[4],lines[6],lines[28])
    return (str(lines[14]),tup)

def check_error(line):
    '''
    根据合并后的交易记录，遍历手机号码的交易记录，查看是否有失败交易，并返回
    :param line:
    :return:
    '''
    for x in line[1]:
        if x[3] != '0000':
            return line

if __name__=='__main__':
    conf = SparkConf().setMaster("spark://192.168.121.193:7077").setAppName("sparktest")
    sc = SparkContext(conf=conf)
    tmall_931_RDD = sc.textFile("hdfs://192.168.121.193:9000/data/tmall/*.931.CMCC.gz")
    tmall_931_RDD.persist()
    num_pairs=tmall_931_RDD.map(check_num_dict)
    num_paris_sub=num_pairs.groupByKey()  # 对有相同的键的值进行合并，也就是相同手机号码的交易记录进行合并
    num_paris_sub.persist()
    num_paris_err=num_paris_sub.map(lambda (key,value):(key,list(value))).filter(check_error)
    num_paris_err.saveAsTextFile("/home/mcbadm/kongcz/search/")