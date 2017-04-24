package org.apache.apex.malhar.python.operator;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PythonFlatMapOperator<T> extends PythonGenericOperator<T>
{
  private static final Logger LOG = LoggerFactory.getLogger(PythonFlatMapOperator.class);

  public PythonFlatMapOperator()
  {
    operationType = OpType.FLAT_MAP;
  }

  public PythonFlatMapOperator(byte[] serializedFunc)
  {
    super(serializedFunc);
    operationType = OpType.FLAT_MAP;
  }

  @Override
  protected void processTuple(T tuple)
  {
    LOG.debug("Received Tuple " + tuple);
    List<T> result = (List<T>)pythonWorkerProxy.execute(tuple);
    if (result != null) {
      LOG.debug("RECEIVED LIST RESULT " + result.getClass());
      if (result instanceof List) {
        for (T item : result) {
          LOG.debug("RECEIVED LIST RESULT " + item);
          out.emit(item);
        }
      }
    }

  }
}
