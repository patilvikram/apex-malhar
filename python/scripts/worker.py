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
import pickle
import pip
from py4j.java_gateway import JavaGateway, CallbackServerParameters, GatewayParameters

class WorkerImpl(object):

    serialized_f = None
    callable_f = None
    def __init__(self, gateway):
        self.gateway = gateway

    def setFunction(self, f):
        print "Setting serialized function",f
        #serialized_f = f.decode('ascii')
        #print serialized_f
        self.callable_f=pickle.loads(f) 
       

    def execute(self,tupleIn ):
        print "Executing for tuple",tupleIn
        return self.callable_f(tupleIn)
    class Java:
        implements = ["org.apache.apex.malhar.python.operator.runtime.PythonWorker"]

def main(argv):
   print (argv[0]) 
   gp = GatewayParameters(address='127.0.0.1',port=int(argv[0])) 
   cb = CallbackServerParameters(daemonize=False,eager_load=True)
   gateway = JavaGateway(gateway_parameters=gp,callback_server_parameters=CallbackServerParameters())
   gateway.entry_point.register( WorkerImpl(gateway)  )
   
if __name__ == "__main__":
    main(sys.argv[1:])

