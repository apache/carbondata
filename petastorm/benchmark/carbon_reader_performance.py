from petastorm.carbon import CarbonDataset
import time


def main():
    # os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
    # os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
    # os.environ['JAVA_HOME'] = '/usr/lib/jvm/jdk1.8.0_181'
    # TODO: fill this in argument
    import jnius_config
    jnius_config.set_classpath(
        "")
    jnius_config.add_options('-Xrs', '-Xmx6096m')
    # jnius_config.add_options('-Xrs', '-Xmx6096m', '-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5555')

    print("Start")
    start = time.time()

    dataset_path = ""

    carbon_dataset = CarbonDataset(dataset_path)

    num = 0

    for piece in carbon_dataset.pieces:
        table = piece.read_all(("imageid", "imagename", "imagebinary", "txtname", "txtcontent"))
        num += len(table)

    end = time.time()
    print("all time: " + str(end - start))
    print("Finish")


if __name__ == '__main__':
    main()