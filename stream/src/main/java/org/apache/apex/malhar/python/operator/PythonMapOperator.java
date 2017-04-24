package org.apache.apex.malhar.python.operator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PythonMapOperator<T> extends PythonGenericOperator<T>
{
  private static final Logger LOG = LoggerFactory.getLogger(PythonMapOperator.class);

  public PythonMapOperator()
  {
    operationType = OpType.MAP;
  }

  public PythonMapOperator(byte[] serializedFunc)
  {
    super(serializedFunc);
    operationType = OpType.MAP;
  }

  @Override
  protected void processTuple(T tuple)
  {
    LOG.info("Received Tuple " + tuple);
    Object result = pythonWorkerProxy.execute(tuple);
    out.emit((T)result);
  }
}
