package org.apache.apex.malhar.python.operator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by vikram on 5/4/17.
 */
public class PythonMapOperator<T> extends PythonGenericOperator<T>
{
  private static final Logger LOG = LoggerFactory.getLogger(PythonMapOperator.class);

  public PythonMapOperator()
  {
    xFormType = OpType.MAP;
  }

  public PythonMapOperator(byte[] serializedFunc)
  {
    super(serializedFunc);
    xFormType = OpType.MAP;
  }

  @Override
  protected void processTuple(T tuple)
  {
    LOG.info("Received Tuple " + tuple);
    Object result = pythonWorkerProxy.execute(tuple);
    out.emit((T)result);
  }
}
