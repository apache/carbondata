# Copyright (c) Huawei Technologies Co., Ltd. 2019-2020. All rights reserved.
# Description: example of how to write a dataset to obs in parquet format
# Create: 2019-01-29
# Author: xinrunxue 00318742

import numpy as np
# import pyarrow as pa
from io import BytesIO
# import pyarrow.parquet as pq
# import pandas as pd
import s3fs
import sys
import time

def main(argv):
    start = time.time()
    file_path='obs-aab3/test3'
    bucket='modelartscarbon/flowers/'
    s3 = connect_obs()
    num = 0
    for file in s3.ls(bucket):
       num += 1
       print(str(num)+":"+file)
       with s3.open(file, mode='rb') as f:
           f.read()
    end = time.time()
    print(end-start)
    print(num)

def connect_obs():
    # access_key = '3HESOBTN6UWUCAJWDGIH'
    # secret_key = 'W8veV8dXYJwSekPHfi5IjQVpoLObSc5jXxAH5aqZ'
    access_key = 'GYLW3KJVB7IFHNF5W3JV'
    secret_key = '6KYRbqkodxzk9xIka743Z5E1Y3nm9KnGsHPuZqPT'
    end_point = 'https://obs.myhwclouds.com'
    s3 = s3fs.S3FileSystem(key=access_key, secret=secret_key, client_kwargs={'endpoint_url':end_point})
    return s3


if __name__ == '__main__':
        main(sys.argv)