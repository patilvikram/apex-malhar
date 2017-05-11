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

    def get_jvm_gateway(self):
        return self.gateway

    def get_entry_point(self):
        return self.gateway.entry_point


def createApp(name):
    shellConnector = ShellConnector()
    return ApexStreamingApp(name, shellConnector)


def getApp(name):
    shellConnector = ShellConnector()
    java_app = shellConnector.get_entry_point().getAppByName(name)

    return ApexStreamingApp(name, java_app=java_app)


'''
This is Python Wrapper Around ApexStreamingApp
If java streaming app is not found then no apis can be called on this wrapper.

'''
class ApexStreamingApp():
    app_id = None
    streaming_factory = None
    java_streaming_app = None
    instance_id = None
    shell_connector = None
    serialized_file_list = []

    def __init__(self, name, shell_connector=None, java_app=None):
        if shell_connector is None and java_app is None:
            raise Exception("Invalid App initialization")
        if java_app is None:
            self.java_streaming_app = shell_connector.get_entry_point().createApp(name)
        else:
            self.java_streaming_app = java_app
        self.shell_connector = shell_connector
        self.instance_id = uuid1().urn[9:]

    '''
        This fuction will initialize input adapter to read from hdfs adapter  
    '''

    def fromFolder(self, folder_path):
        self.java_streaming_app = self.java_streaming_app.fromFolder(folder_path)
        return self

    def fromKafka08(self, zoopkeepers, topic):
        self.java_streaming_app = self.java_streaming_app.fromKafka08(zoopkeepers, topic)
        return self

    def fromKafka09(self, zoopkeepers, topic):
        self.java_streaming_app = self.java_streaming_app.fromKafka09(zoopkeepers, topic)
        return self
    def toConsole(self, name=None):
        self.java_streaming_app = self.java_streaming_app.toConsole(name)
        return self

    def toKafka08(self, name=None, topic=None, brokerList=None, **kwargs):
        properties = {}
        if brokerList is not None:
            properties['bootstrap.servers'] = brokerList
        for key in kwargs.keys():
            properties[key] = kwargs[key]
        property_map = self.shell_connector.get_jvm_gateway().jvm.java.util.HashMap()
        for key in properties.keys():
            property_map.put(key, properties[key])
        self.java_streaming_app = self.java_streaming_app.toKafka08(name, topic, property_map)
        return self

    def toFolder(self, name,file_name, directory_name, **kwargs):
        properties = {}
        if file_name is None or directory_name is None:
            raise Exception("Directory Name or File name should be specified")
        self.java_streaming_app = self.java_streaming_app.toFolder( name, file_name, directory_name )
        return self

    def map(self, name, func):
        if not isinstance(func, types.FunctionType):
            raise Exception

        serialized_func = self.get_serialized_file_name(name, func)
        self.java_streaming_app = self.java_streaming_app.setMap(name, serialized_func)
        return self

    def setMap(self, name, func):
        if not isinstance(func, types.FunctionType):
            raise Exception
        serialized_func  = self.get_serialized_file_name(name, func)
        self.java_streaming_app = self.java_streaming_app.setMap(name, serialized_func)
        return self

    def setFlatMap(self, name, func, ):
        if not isinstance(func, types.FunctionType):
            raise Exception

        serialized_func  = self.get_serialized_file_name(name, func)
        self.java_streaming_app = self.java_streaming_app.setFlatMap(name, serialized_func)
        return self

    def setFilter(self, name, func):
        if not isinstance(func, types.FunctionType):
            raise Exception
        serialized_func = self.get_serialized_file_name(name, func)
        self.java_streaming_app = self.java_streaming_app.setFilter(name, serialized_func)
        return self

    def fromData(self, data):
        if not isinstance(data, list):
            raise Exception
        data_for_java = self.shell_connector.get_jvm_gateway().jvm.java.util.ArrayList()
        types_data = [int, float, str, bool, tuple, dict]
        for d in data:
            if type(d) in types_data:
                data_for_java.append(d)
        self.java_streaming_app = self.java_streaming_app.fromData(data_for_java)
        return self

    def launch(self, local_mode=False):
        try:
            self.app_id = self.java_streaming_app.launch(local_mode)
            return self.app_id
        except Py4JJavaError as e:
            import traceback
            traceback.print_exc()
            print e.java_exception.getMessage()

    def kill(self):
        return self.java_streaming_app.kill()

    def setConfig(self, key, value):
        self.java_streaming_app = self.java_streaming_app.setConfig(key, value)
        return self

    def get_serialized_file_name(self, name, func):

        serialized_func = bytearray()
        serialized_func.extend(cloudpickle.dumps(func))
        # temp_file = NamedTemporaryFile()
        # temp_file.write(serialized_func)
        # temp_file.name = "pythonapp_ " + self.instance_id + "_opr_" + name + ".ser"
        return serialized_func
