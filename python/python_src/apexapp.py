#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import types
from py4j.java_gateway import JavaGateway
import cloudpickle
from tempfile import TemporaryFile, NamedTemporaryFile
from uuid import uuid1
from py4j.protocol import Py4JJavaError


class ShellConnector(object):
    gateway = None
    entry_point = None

    def __init__(self):
        self.gateway = JavaGateway()

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(ShellConnector, cls).__new__(cls)

        return cls.instance

    def get_entry_point(self):
        return self.gateway.entry_point


def createApp(name):
    shellConnector = ShellConnector()
    return ApexStreamingApp(name, shellConnector)


class ApexStreamingApp():
    app_id = None
    streaming_factory = None
    apex_stream = None
    java_streaming_app = None
    instance_id = None

    serialized_file_list = []

    def __init__(self, name, shell_connector):
        self.java_streaming_app = shell_connector.get_entry_point().createApp(name)
        self.instance_id = uuid1().urn[9:]

    '''
        This fuction will initialize input adapter to read from hdfs adapter  
    '''

    def fromFolder(self, folder_path):
        self.java_streaming_app = self.java_streaming_app.fromFolder(folder_path)
        return self

    def toConsole(self, name=None):
        self.java_streaming_app = self.java_streaming_app.toConsole(name)
        return self

    def map(self, name, func):
        if not isinstance(func, types.FunctionType):
            raise Exception
        serialized_func = bytearray()
        serialized_func.extend(cloudpickle.dumps(func))
        t = NamedTemporaryFile(delete=False)
        t.write(serialized_func)
        t.name = "pythonapp_" + self.instance_id + "_opr_" + name + ".ser"
        self.serialized_file_list.append(t)
        self.java_streaming_app = self.java_streaming_app.setMap(name, serialized_func)
        return self

    def setMap(self, name, func):
        if not isinstance(func, types.FunctionType):
            raise Exception
        serialized_func = bytearray()
        serialized_func.extend(cloudpickle.dumps(func))
        t = NamedTemporaryFile(delete=False)
        t.write(serialized_func)
        t.name = "pythonapp_" + self.instance_id + "_opr_" + name + ".ser"
        self.serialized_file_list.append(t)
        print self.serialized_file_list, t.name
        self.java_streaming_app = self.java_streaming_app.setMap(name, serialized_func)
        return self

    def setFlatMap(self, name, func):
        if not isinstance(func, types.FunctionType):
            raise Exception

        # def flat_map_wrapper(tuple):
        #     result = func(tuple )
        #     print "RESULT VALUE BEFORE CONVERSION", result
        #     from py4j.java_collections import SetConverter, MapConverter, ListConverter
        #     return ListConverter().convert(result, self.gateway._gateway_client)

        serialized_func = bytearray()
        serialized_func.extend(cloudpickle.dumps(func))
        # temp_file = NamedTemporaryFile()
        # temp_file.write(serialized_func)
        # temp_file.name = "pythonapp_ " + self.instance_id + "_opr_" + name + ".ser"
        print serialized_func
        self.java_streaming_app = self.java_streaming_app.setFlatMap(name, serialized_func)
        return self

    def setFilter(self, name, func):
        if not isinstance(func, types.FunctionType):
            raise Exception
        serialized_func = bytearray()
        serialized_func.extend(cloudpickle.dumps(func))
        # temp_file = NamedTemporaryFile()
        # temp_file.write(serialized_func)
        # temp_file.name = "pythonapp_ " + self.instance_id + "_opr_" + name + ".ser"
        self.java_streaming_app = self.java_streaming_app.setFilter(name, serialized_func)
        return self

    def launch(self):
        try:
            self.app_id = self.java_streaming_app.launch()
            return self.app_id
        except Py4JJavaError as e:
            print e.java_exception.getMessage()

    def setConfig(self, key, value):
        self.java_streaming_app = self.java_streaming_app.setConfig(key, value)
        return self
