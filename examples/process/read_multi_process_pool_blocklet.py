from multiprocessing import Pool
import os
import time
from pycarbon.CarbonReader import CarbonReader

start = time.time()

process_num = 2

# CARBON_DIR = "/Users/xubo/Desktop/xubo/git/carbondata1/store/sdk/target/flowers"
# CARBON_DIR = "/Users/xubo/Desktop/xubo/data/VOCdevkit/carbon/voc"
CARBON_DIR = "/Users/xubo/Desktop/xubo/git/carbondata1/store/sdk/target/flowers"

carbon_splits = []


def getBlocklet():
  global carbon_splits
  carbon_splits = CarbonReader().builder() \
    .withFolder(CARBON_DIR) \
    .getSplits()
  print(len(carbon_splits))

def consumer(idx):
  print("consumer " + str(idx))
  index = 0
  num = 0
  while True:
    if index == num_blocklet:
      break
    elif index % process_num != idx:
      index += 1
      continue

    # CarbonReader
    # reader2 = CarbonReader
    # .builder()
    # //.filterEqual("txtContent", "roses")
    # .withFolder(path)
    # .withBatch(1000)
    # .buildWithSplits(inputSplits[index]);

    path = "/Users/xubo/Desktop/xubo/git/carbondata1/store/sdk/target/flowers"
    reader = CarbonReader().builder().withFolder(path).build_with_split(carbon_splits[index])
    result = []
    while (reader.hasNext()):
      row = reader.readNextRow()
      num += 1
      result.append(row)

    reader.close()
    index += 1
  return num


if __name__ == "__main__":
  #TODO: support multiple process
  import jnius_config

  jnius_config.set_classpath('/Users/xubo/Desktop/xubo/git/carbondata1/store/sdk/target/carbondata-sdk.jar')
  jnius_config.add_options('-Xrs', '-Xmx6096m')

  print("START")

  getBlocklet()
  num_blocklet = len(carbon_splits)
  print(num_blocklet)
  print(carbon_splits)

  pool = Pool(processes=process_num)
  result = []
  for i in range(process_num):
    result.append(pool.apply_async(consumer, args=(i,)))

  pool.close()
  pool.join()

  num = 0
  for i in result:
    num += i.get()

  end = time.time()
  print("all time " + str(end - start))
  print("END " + str(num))
