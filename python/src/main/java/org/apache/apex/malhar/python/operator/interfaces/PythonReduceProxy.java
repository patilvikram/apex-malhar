package org.apache.apex.malhar.python.operator.interfaces;

import org.apache.apex.malhar.python.runtime.PythonAccumulatorWorker;

/**
 * Created by vikram on 27/6/17.
 */
public class PythonReduceProxy<T> extends PythonAcummlationWorkerProxy<T> implements Reduce<T>
{
  public PythonReduceProxy(byte[] serializedFunc)
  {
    super(serializedFunc);
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
    return (T)((PythonAccumulatorWorker)getWorker()).accumulate(input1,input2);
  }
}
