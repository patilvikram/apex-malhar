package org.apache.apex.malhar.python.operator.proxy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.PythonConstants;
import org.apache.apex.malhar.lib.window.accumulation.Reduce;
import org.apache.apex.malhar.python.operator.interfaces.PythonReduceWorker;

public class PythonReduceProxy<T> extends PythonAcummlationWorkerProxy<T> implements Reduce<T>
{
  private static final Logger LOG = LoggerFactory.getLogger(PythonReduceProxy.class);

  public PythonReduceProxy()
  {
    super();
  }

  public PythonReduceProxy(PythonConstants.OpType operationType, byte[] serializedFunc)
  {
    super(serializedFunc);
    this.operationType = operationType;
  }

  @Override
  public T defaultAccumulatedValue()
  {
    return null;
  }

  @Override
  public T getOutput(T accumulatedValue)
  {
    return accumulatedValue;
  }


  @Override
  public T getRetraction(T value)
  {
    return null;
  }

  @Override
  public T accumulate(T accumulatedValue, T input)
  {
    if (accumulatedValue == null) {
      return input;
    }
    return reduce(accumulatedValue, input);
  }

  @Override
  public T merge(T accumulatedValue1, T accumulatedValue2)
  {
    return reduce(accumulatedValue1, accumulatedValue2);
  }

  @Override
  public T reduce(T input1, T input2)
  {
    LOG.debug("Input received {} {} ", input1,input2);

   T result= (T)((PythonReduceWorker)getWorker()).reduce(input1,input2);
    LOG.debug("Output received {}", result );
   return result;
  }
}
