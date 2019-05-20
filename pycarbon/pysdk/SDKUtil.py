class SDKUtil(object):
  def __init__(self):
    from jnius import autoclass
    self.SDKUtilClass = autoclass('org.apache.carbondata.sdk.file.utils.SDKUtil')

  def readBinary(self, path):
    return self.SDKUtilClass.readBinary(path)
