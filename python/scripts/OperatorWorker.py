import sys
from py4j.java_gateway import JavaGateway, CallbackServerParameters, GatewayParameters
import pickle

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
        implements = ["org.apache.apex.malhar.stream.api.python.PythonWorker"]

def main(argv):
   print (argv[0]) 
   gp = GatewayParameters(address='127.0.0.1',port=int(argv[0])) 
   cb = CallbackServerParameters(daemonize=False,eager_load=True)
   gateway = JavaGateway(gateway_parameters=gp,callback_server_parameters=CallbackServerParameters())
   gateway.entry_point.register( WorkerImpl(gateway)  )
   
if __name__ == "__main__":
    main(sys.argv[1:])
