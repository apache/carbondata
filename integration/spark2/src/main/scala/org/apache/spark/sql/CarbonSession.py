from __future__ import print_function
import sys
from threading import RLock

if sys.version >= '3':
    basestring = unicode = str
    xrange = range
else:
    from itertools import izip as zip, imap as map

from pyspark.sql.conf import RuntimeConfig

__all__ = ["CarbonSession"]


class CarbonSession(object):
    class Builder(object):

        _options = {}
        _lock = RLock()
        _useHiveMetaStore = True

        def config(self, key=None, value=None, conf=None):
            with self._lock:
                if conf is None:
                    self._options[key] = str(value)
                else:
                    for (k, v) in conf.getAll():
                        self._options[k] = v
                return self

        def master(self, master):
            return self.config("spark.master", master)

        def appName(self, name):
            return self.config("spark.app.name", name)

        def enableHiveSupport(self):
            return self.config("spark.sql.catalogImplementation", "hive")

        def getOrCreateCarbonSession(self, storePath=None, metaStorePath=None):
            with self._lock:
                from pyspark.context import SparkContext
                from pyspark.conf import SparkConf
                carbonsession = CarbonSession._instantiatedSession
                if metaStorePath is not None:
                    self.config("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=" + metaStorePath + "/metastore_db;create=true")

                if carbonsession is None or carbonsession._sc._jsc is None:
                    if self._sc is not None:
                        sc = self._sc
                    else:
                        sparkConf = SparkConf()
                        for key, value in self._options.items():
                            sparkConf.set(key, value)
                        # This SparkContext may be an existing one.
                        sc = SparkContext.getOrCreate(sparkConf)
                    # Do not update `SparkConf` for existing `SparkContext`, as it's shared
                    # by all sessions.
                    carbonsession = CarbonSession(sc)
                for key, value in self._options.items():
                    carbonsession._jCarbonSession.sessionState().conf().setConfString(key, value)
                return carbonsession

    builder = Builder()
    """A class attribute having a :class:`Builder` to construct :class:`CarbonSession` instances"""

    _instantiatedSession = None

    def __init__(self, sparkContext, jCarbonSession=None):
        from pyspark.sql.context import SQLContext
        self._sc = sparkContext
        self._jsc = self._sc._jsc
        self._jvm = self._sc._jvm
        if jCarbonSession is None:
            existingSession = self._jvm.SparkSession.getActiveSession()
            if existingSession.isDefined() and not existingSession.get().sparkContext().isStopped():
                jCarbonSession = self._jvm.SparkSession.getActiveSession().get()
            elif self._jvm.SparkSession.getDefaultSession().isDefined() \
                        and not self._jvm.SparkSession.getDefaultSession().get() \
                        .sparkContext().isStopped():
                    jCarbonSession = self._jvm.SparkSession.getDefaultSession().get()
            else:
                jCarbonSession = self._jvm.CarbonSession(self._jsc.sc())
        self._jCarbonSession = jCarbonSession
        self._jwrapped = self._jCarbonSession.sqlContext()
        self._wrapped = SQLContext(self._sc, self, self._jwrapped)
        if CarbonSession._instantiatedSession is None \
                or CarbonSession._instantiatedSession._sc._jsc is None:
            CarbonSession._instantiatedSession = self
        self._jvm.SparkSession.setDefaultSession(self._jCarbonSession)


    def newSession(self):
        return self.__class__(self._sc, self._jCarbonSession.newSession())

    @property
    def sparkContext(self):
        return self._sc

    @property
    def version(self):
        return self._jCarbonSession.version()

    @property
    def conf(self):
        if not hasattr(self, "_conf"):
            self._conf = RuntimeConfig(self._jCarbonSession.conf())
        return self._conf

    @property
    def catalog(self):
        from pyspark.sql.catalog import Catalog
        if not hasattr(self, "_catalog"):
            self._catalog = Catalog(self)
        return self._catalog






