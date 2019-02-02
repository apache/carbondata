from obs import *
import sys
import time

def main(argv):
    start = time.time()
    read_obs_files(argv[1], argv[2], argv[3],
      'modelartscarbon', 'voc/vocImageAndAnnotation1000/vocImageAndAnnotation1000/')
    end = time.time()
    print(end - start)

def list_obs_files(obs_client, bucket_name,prefix):
    files = []

    pageSize = 1000
    index = 1
    nextMarker = None
    while True:
        resp = obs_client.listObjects(bucket_name, prefix=prefix,max_keys=pageSize, marker=nextMarker)
        for content in resp.body.contents:
            # print(content.key)
            files.append(content.key)
        if not resp.body.is_truncated:
            break
        nextMarker = resp.body.next_marker
        index += 1

    return files


def read_obs_files(access_key, secret_key, end_point, bucket_name,prefix):
    obsClient = ObsClient(
        access_key_id=access_key,
        secret_access_key=secret_key,
        server=end_point,
        long_conn_mode=True
    )
    files = list_obs_files(obsClient, bucket_name, prefix)
    numOfFiles=len(files)
    print(numOfFiles)
    images = []
    num = 0
    for file in files:
        num=num+1
        # obsClient.l
        resp = obsClient.getObject(bucket_name, file, loadStreamInMemory=True)
        resp.body.buffer
        if(0==num%(numOfFiles/10)):
            print(str(num)+":"+file)
    obsClient.close()

if __name__ == '__main__':
    main(sys.argv)

