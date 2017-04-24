package org.apache.apex.malhar.python.operator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultOutputPort;

public class PythonFilterOperator<T> extends PythonGenericOperator<T>
{
  private static final Logger LOG = LoggerFactory.getLogger(PythonFilterOperator.class);

  DefaultOutputPort<T> falsePort = new DefaultOutputPort<>();
  DefaultOutputPort<T> truePort = new DefaultOutputPort<T>();

  public PythonFilterOperator()
  {
    operationType = OpType.FILTER;
  }

  public PythonFilterOperator(byte[] serializedFunc)
  {
    super(serializedFunc);
    operationType = OpType.FILTER;
  }

  @Override
  protected void processTuple(T tuple)
  {
    LOG.debug("Received Tuple " + tuple);
    Object result = pythonWorkerProxy.execute(tuple);
    LOG.debug("RECEIVED LIST RESULT " + result);
    if (result instanceof Boolean) {
      Boolean b = (Boolean)result;
      LOG.debug("FILTER RESPONSE RECEIVED " + b);
      if ( b.booleanValue()) {
        out.emit(tuple);
      }
    }

  }
}
