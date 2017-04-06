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

import sys
import site
import pip
from py4j.java_gateway import JavaGateway, CallbackServerParameters, GatewayParameters
import logging

gateway = None


class WorkerImpl(object):
    serialized_f = None
    callable_f = None
    xformType = None

    def __init__(self, gateway, xformType):
        self.gateway = gateway
        self.xformType = xformType

    def setFunction(self, f, xformType):
        try:
            import os, imp

            print "Setting serialized function dcoding 1"
            print os.environ['PYTHONPATH']
            print imp.find_module('cloudpickle')
            import cloudpickle
            # serialized_f = f.decode('ascii')
            print "Decoded serialized funcion", xformType

            self.callable_f = cloudpickle.loads(f)
            self.xformType = xformType
            print "Serialized function set correctly"
        except ValueError as e:
            print str(e)
            from traceback import print_exc
            print_exc()
        except Exception:
            from traceback import print_exc
            print_exc()
        return "RETURN VALUE"

    def execute(self, tupleIn):
        print "Executing for tuple", tupleIn, self.xformType
        try:
            result = self.callable_f(tupleIn)
            if self.xformType == 'MAP':
                return result
            elif self.xformType == 'FLAT_MAP':
                from py4j.java_collections import SetConverter, MapConverter, ListConverter
                return ListConverter().convert(result, self.gateway._gateway_client)

            elif self.xformType == 'FILTER':
                if type(result) != bool:
                        result = True if result is not None else False
                return result


            print "Result value", result
            return result
        except ValueError as e:
            print str(e)
            from traceback import print_exc
            print_exc()
        except Exception:
            from traceback import print_exc
            print_exc()
        return None

    class Java:
        implements = ["org.apache.apex.malhar.python.operator.runtime.PythonWorker"]

# TODO this may cause race condition
def find_free_port():
    import socket
    s = socket.socket()
    s.listen(0)
    addr, found_port= s.getsockname()  # Return the port number assigned.
    print "FOUND AVAILABLE PORT ", found_port
    s.shutdown(socket.SHUT_RDWR)
    s.close()
    return found_port

def main(argv):
    import os, getpass
    print argv
    print [f for f in sys.path if f.endswith('packages')]

    print os.environ['HOME']
    print site.ENABLE_USER_SITE
    print site.USER_SITE
    print site.USER_BASE
    print ("USER SITE PACKAGE" + site.getusersitepackages())
    PYTHON_PATH = os.environ['PYTHONPATH'] if 'PYTHONPATH' in os.environ else None
    os.environ['PYTHONPATH'] = PYTHON_PATH + ':' + site.getusersitepackages().replace('/home/.local/',
                                                                                      '/home/' + getpass.getuser() + '/.local/') + '/'
    sys.path.extend(os.environ['PYTHONPATH'].split(':'))
    print os.environ['PYTHONPATH']
    import pickle
    gp = GatewayParameters(address='127.0.0.1', port=int(argv[0]), auto_convert=True)
    cb = CallbackServerParameters(daemonize=False, eager_load=True,port= 0)
    gateway = JavaGateway(gateway_parameters=gp, callback_server_parameters=cb)

    # retrieve the port on which the python callback server was bound to.
    python_port = gateway.get_callback_server().get_listening_port()
    print "PYTHON CALLBACK SERVER LISTENING PORT "+ str(python_port)

    # tell the Java side to connect to the python callback server with the new
    # python port. Note that we use the java_gateway_server attribute that
    # retrieves the GatewayServer instance.
    gateway.java_gateway_server.resetCallbackClient(
        gateway.java_gateway_server.getCallbackClient().getAddress(),
        python_port)


    gateway.entry_point.register(WorkerImpl(gateway, argv[1]))
    print getpass.getuser()


if __name__ == "__main__":
    main(sys.argv[1:])
