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

class WorkerImpl(object):

    serialized_f = None
    callable_f = None
    def __init__(self, gateway):
        self.gateway = gateway

    def setFunction(self, f):
	try:
           import os, imp
  
           print "Setting serialized function dcoding 1"
           print os.environ['PYTHONPATH']
           print imp.find_module( 'cloudpickle')
           import cloudpickle
           #serialized_f = f.decode('ascii')
           print "Decoded serialized funcion", 

           self.callable_f=cloudpickle.loads(f) 
           print "Serialized function set correctly"
	except ValueError as e:
	   print str(e)
           from traceback import print_exc
           print_exc()
        except Exception:
           from traceback import print_exc
           print_exc()
      	return "RETURN VALUE"

    def execute(self,tupleIn ):
        print "Executing for tuple",tupleIn
	try:
           result  =self.callable_f(tupleIn)
           print "Generated for tuple", result
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

def main(argv):

   import os,getpass
   print [f for f in sys.path if f.endswith('packages')]

   
   print os.environ['HOME'] 
   print site.ENABLE_USER_SITE
   print site.USER_SITE
   print site.USER_BASE
   print ("USER SITE PACKAGE"+site.getusersitepackages())   
   PYTHON_PATH = os.environ['PYTHONPATH']  if 'PYTHONPATH' in os.environ else None
   os.environ['PYTHONPATH']= PYTHON_PATH+':' + site.getusersitepackages().replace('/home/.local/','/home/'+getpass.getuser()+'/.local/')+'/'
   sys.path.extend( os.environ['PYTHONPATH'].split(':'))
   print os.environ['PYTHONPATH']
   import pickle
   gp = GatewayParameters(address='127.0.0.1',port=int(argv[0])) 
   cb = CallbackServerParameters(daemonize=False,eager_load=True)
   gateway = JavaGateway(gateway_parameters=gp,callback_server_parameters=CallbackServerParameters())
   gateway.entry_point.register( WorkerImpl(gateway)  )
   print getpass.getuser()
   
if __name__ == "__main__":
    main(sys.argv[1:])

