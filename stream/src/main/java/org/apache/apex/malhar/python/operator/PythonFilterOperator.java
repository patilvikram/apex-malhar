package org.apache.apex.malhar.python.operator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultOutputPort;

/**
 * Created by vikram on 5/4/17.
 */
public class PythonFilterOperator<T> extends PythonGenericOperator<T>
{
  private static final Logger LOG = LoggerFactory.getLogger(PythonFilterOperator.class);

  DefaultOutputPort<T> falsePort = new DefaultOutputPort<>();
  DefaultOutputPort<T> truePort = new DefaultOutputPort<T>();

  public PythonFilterOperator()
  {
    xFormType = OpType.FILTER;
  }

  public PythonFilterOperator(byte[] serializedFunc)
  {
    super(serializedFunc);
    xFormType = OpType.FILTER;
  }

  @Override
  protected void processTuple(T tuple)
  {
    LOG.info("Received Tuple " + tuple);
    Object result = pythonWorkerProxy.execute(tuple);
    LOG.info("RECEIVED LIST RESULT " + result);
    if (result instanceof Boolean) {
      Boolean b = (Boolean)result;
      LOG.info("FILTER RESPONSE RECEIVED " + b);
      if ( b.booleanValue()) {
        out.emit(tuple);
      }
    }

  }
}
