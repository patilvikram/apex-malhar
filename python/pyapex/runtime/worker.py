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



'''
Worker.py file is responsible for instantiating specific workers such as MapWorkerImpl, FlatMapWorkerImpl, FilterWorkerImpl.

Worker.py is ran using python and then we register back WorkerImpl with Java Process for each calls. 


'''
import sys
import site
import pip
from py4j.java_gateway import JavaGateway, CallbackServerParameters, GatewayParameters, java_import
import logging
from abc import ABCMeta, abstractmethod

gateway = None


class AbstractPythonWorker(object):
  __metaclass__ = ABCMeta

  @abstractmethod
  def setFunction(self, f, opType):
    pass

  @abstractmethod
  def execute(self, tupleIn):
    pass

  @abstractmethod
  def setState(self, dataMap):
    pass

  class Java:
    implements = ["org.apache.apex.malhar.python.runtime.PythonWorker"]


class WorkerImpl(AbstractPythonWorker):
  serialized_f = None
  callable_f = None
  opType = None
  dataMap = None
  counter = 0;

  def __init__(self, gateway, opType):
    self.gateway = gateway
    self.opType = opType

  def setFunction(self, f, opType):
    try:
      import os, imp
      import cloudpickle
      self.callable_f = cloudpickle.loads(f)
      self.opType = opType
    except ValueError as e:
      print str(e)
      from traceback import print_exc
      print_exc()
    except Exception:
      from traceback import print_exc
      print_exc()
    return "RETURN VALUE"

  def setState(self, dataMap):
    self.dataMap = dataMap
    print "Python : setting state correctly"
    return True

  def factory(gateway, type):
    # return eval(type + "()")
    if type == "MAP": return MapWorkerImpl(gateway, type)
    if type == "FLATMAP": return FlatMapWorkerImpl(gateway, type)
    if type == "FILTER": return FilterWorkerImpl(gateway, type)
    assert 0, "Bad shape creation: " + type

  factory = staticmethod(factory)


class MapWorkerImpl(WorkerImpl):
  def execute(self, tupleIn):
    try:
      result = self.callable_f(tupleIn)
      return result
    except ValueError as e:
      print str(e)
      from traceback import print_exc
      print_exc()
    except Exception:
      from traceback import print_exc
      print_exc()
    return None


class FlatMapWorkerImpl(WorkerImpl):
  def execute(self, tupleIn):
    try:
      result = self.callable_f(tupleIn)
      from py4j.java_collections import SetConverter, MapConverter, ListConverter
      return ListConverter().convert(result, self.gateway._gateway_client)
      return result
    except ValueError as e:
      print str(e)
      from traceback import print_exc
      print_exc()
    except Exception:
      from traceback import print_exc
      print_exc()
    return None


class FilterWorkerImpl(WorkerImpl):
  def execute(self, tupleIn):
    try:
      result = self.callable_f(tupleIn)
      if type(result) != bool:
        result = True if result is not None else False
      return result
    except ValueError as e:
      print str(e)
      from traceback import print_exc
      print_exc()
    except Exception:
      from traceback import print_exc
      print_exc()
    return None


# TODO this may cause race condition
def find_free_port():
  import socket
  s = socket.socket()
  s.listen(0)
  addr, found_port = s.getsockname()  # Return the port number assigned.
  s.shutdown(socket.SHUT_RDWR)
  s.close()
  return found_port


def main(argv):
  import os, getpass
  # print argv
  # print [f for f in sys.path if f.endswith('packages')]

  PYTHON_PATH = os.environ['PYTHONPATH'] if 'PYTHONPATH' in os.environ else None
  os.environ['PYTHONPATH'] = PYTHON_PATH + ':' + site.getusersitepackages().replace('/home/.local/',
                                                                                    '/home/' + getpass.getuser() + '/.local/') + '/'
  sys.path.extend(os.environ['PYTHONPATH'].split(':'))
  print "PYTHONPATH " + str(os.environ['PYTHONPATH'])
  import pickle
  gp = GatewayParameters(address='127.0.0.1', port=int(argv[0]), auto_convert=True)
  cb = CallbackServerParameters(daemonize=False, eager_load=True, port=0)
  gateway = JavaGateway(gateway_parameters=gp, callback_server_parameters=cb)

  # Retrieve the port on which the python callback server was bound to.
  python_port = gateway.get_callback_server().get_listening_port()

  # Register python callback server with java gateway server
  # Note that we use the java_gateway_server attribute that
  # retrieves the GatewayServer instance.
  gateway.java_gateway_server.resetCallbackClient(
    gateway.java_gateway_server.getCallbackClient().getAddress(),
    python_port)

  # Instantiate WorkerImpl for PythonWorker java interface and regsiter with PythonWorkerProxy in Java.
  workerImpl = WorkerImpl.factory(gateway, argv[1])
  gateway.entry_point.register(workerImpl)
  print "Python process started with type" + argv[1]


if __name__ == "__main__":
  main(sys.argv[1:])
