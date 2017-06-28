
from abc import ABCMeta, abstractmethod

class AbstractAccumulatorPythonWorker(object):
  __metaclass__ = ABCMeta


  @abstractmethod
  def setObject(self, obj, opType):
    pass

  @abstractmethod
  def defaultAccumulatedValue(self):
    pass


  @abstractmethod
  def getOutput(accumulated):
    pass


  @abstractmethod
  def getRetraction(output):
    pass

  @abstractmethod
  def accumulate(self, accumulated, input):
    pass

  @abstractmethod
  def merge(self, input1, input2):
    pass

  # class Java:
  #   implements = ["org.apache.apex.malhar.python.runtime.PythonAccumulatorWorker"]


class AccumulatorWorkerImpl(AbstractAccumulatorPythonWorker):

  accum_obj = None
  opType = None
  counter = 0;

  def __init__(self, gateway, opType):
    self.gateway = gateway
    self.opType = opType

  @abstractmethod
  def setObject(self, obj, opType):
    try:
      import os, imp
      import cloudpickle
      self.accum_obj = cloudpickle.loads(obj)
      self.opType = opType
    except ValueError as e:
      print str(e)
      from traceback import print_exc
      print_exc()
    except Exception:
      from traceback import print_exc
      print_exc()
    return "RETURN VALUE"


  def getConfirmed(self):
    return self.opType


class ReduceFunction(AbstractAccumulatorPythonWorker):

  def accumulate( self, accumulated, data ):
    return self.reduce(accumulated, data)


  def merge( self, input1, input2 ):
    return self.reduce(input1, input2)

  @abstractmethod
  def reduce( input1, input2 ):
    pass

  def defaultAccumulatedValue( self, data ):
    return data

  def getOutput( accumulated ):
    return accumulated

  def getRetraction( output ):
    return None

  def setObject( self, obj, opType ):
    pass

  class Java:
    implements = ["org.apache.apex.malhar.python.operator.interfaces.PythonReduceWorker"]