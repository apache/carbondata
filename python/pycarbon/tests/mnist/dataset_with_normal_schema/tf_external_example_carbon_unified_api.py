# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from __future__ import division, print_function

import argparse
import os
import time

import jnius_config
import tensorflow as tf

from pycarbon.reader import make_reader
from pycarbon.reader import make_dataset

from pycarbon.tests.mnist.dataset_with_normal_schema import DEFAULT_MNIST_DATA_PATH
from pycarbon.tests import DEFAULT_CARBONSDK_PATH


def decode(carbon_record):
  """Parses an image and label from the given `carbon_reader`."""

  image_raw = getattr(carbon_record, 'image')
  image = tf.decode_raw(image_raw, tf.uint8)

  label_raw = getattr(carbon_record, 'digit')
  label = tf.cast(label_raw, tf.int64)

  return label, image


def train_and_test(dataset_url, num_epochs, batch_size, evaluation_interval):
  """
  Train a model for training iterations with a batch size batch_size, printing accuracy every log_interval.
  :param dataset_url: The MNIST dataset url.
  :param num_epochs: The number of epochs to train for.
  :param batch_size: The batch size for training.
  :param evaluation_interval: The interval used to print the accuracy.
  :return:
  """

  with make_reader(os.path.join(dataset_url, 'train'), num_epochs=num_epochs) as train_reader:
    with make_reader(os.path.join(dataset_url, 'test'), num_epochs=num_epochs) as test_reader:
      # Create the model
      x = tf.placeholder(tf.float32, [None, 784])
      w = tf.Variable(tf.zeros([784, 10]))
      b = tf.Variable(tf.zeros([10]))
      y = tf.matmul(x, w) + b

      # Define loss and optimizer
      y_ = tf.placeholder(tf.int64, [None])

      # Define the loss function
      cross_entropy = tf.losses.sparse_softmax_cross_entropy(labels=y_, logits=y)

      train_step = tf.train.GradientDescentOptimizer(0.5).minimize(cross_entropy)

      correct_prediction = tf.equal(tf.argmax(y, 1), y_)

      accuracy = tf.reduce_mean(tf.cast(correct_prediction, tf.float32))

      train_dataset = make_dataset(train_reader) \
        .apply(tf.data.experimental.unbatch()) \
        .batch(batch_size) \
        .map(decode)

      train_iterator = train_dataset.make_one_shot_iterator()
      label, image = train_iterator.get_next()

      test_dataset = make_dataset(test_reader) \
        .apply(tf.data.experimental.unbatch()) \
        .batch(batch_size) \
        .map(decode)

      test_iterator = test_dataset.make_one_shot_iterator()
      test_label, test_image = test_iterator.get_next()

      # Train
      print('Training model for {0} epoch with batch size {1} and evaluation interval {2}'.format(
        num_epochs, batch_size, evaluation_interval
      ))

      i = 0
      with tf.Session() as sess:
        sess.run([
          tf.local_variables_initializer(),
          tf.global_variables_initializer(),
        ])

        try:
          while True:
            cur_label, cur_image = sess.run([label, image])

            sess.run([train_step], feed_dict={x: cur_image, y_:cur_label})

            if i % evaluation_interval == 0:
              test_cur_label, test_cur_image = sess.run([test_label, test_image])
              print('After {0} training iterations, the accuracy of the model is: {1:.2f}'.format(
                i,
                sess.run(accuracy, feed_dict={
                  x: test_cur_image, y_: test_cur_label})))
            i += 1

        except tf.errors.OutOfRangeError:
          print("Finish! the number is " + str(i))


def main():
  print("Start")
  start = time.time()

  # Training settings
  parser = argparse.ArgumentParser(description='Pycarbon Tensorflow External MNIST Example')
  default_dataset_url = 'file://{}'.format(DEFAULT_MNIST_DATA_PATH)
  parser.add_argument('--dataset-url', type=str,
                      default=default_dataset_url, metavar='S',
                      help='hdfs:// or file:/// URL to the MNIST pycarbon dataset'
                           '(default: %s)' % default_dataset_url)
  parser.add_argument('--num-epochs', type=int, default=1, metavar='N',
                      help='number of epochs to train (default: 1)')
  parser.add_argument('--batch-size', type=int, default=100, metavar='N',
                      help='input batch size for training (default: 100)')
  parser.add_argument('--evaluation-interval', type=int, default=10, metavar='N',
                      help='how many batches to wait before evaluating the model accuracy (default: 10)')
  parser.add_argument('--carbon-sdk-path', type=str, default=DEFAULT_CARBONSDK_PATH,
                      help='carbon sdk path')

  args = parser.parse_args()

  jnius_config.set_classpath(args.carbon_sdk_path)

  train_and_test(
    dataset_url=args.dataset_url,
    num_epochs=args.num_epochs,
    batch_size=args.batch_size,
    evaluation_interval=args.evaluation_interval
  )
  end = time.time()
  print("all time: " + str(end - start))
  print("Finish")


if __name__ == '__main__':
  main()
