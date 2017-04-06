package org.apache.apex.malhar.python.operator;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by vikram on 5/4/17.
 */
public class PythonFlatMapOperator<T> extends PythonGenericOperator<T>
{
  private static final Logger LOG = LoggerFactory.getLogger(PythonFlatMapOperator.class);

  public PythonFlatMapOperator()
  {
    xFormType = OpType.FLAT_MAP;
  }

  public PythonFlatMapOperator(byte[] serializedFunc)
  {
    super(serializedFunc);
    xFormType = OpType.FLAT_MAP;
  }

  @Override
  protected void processTuple(T tuple)
  {
    LOG.info("Received Tuple " + tuple);
    List<T> result = (List<T>)pythonWorkerProxy.execute(tuple);
    LOG.info("RECEIVED LIST RESULT " + result.getClass());
    if (result instanceof List) {
      for (T item : result) {
        LOG.info("RECEIVED LIST RESULT " + item);
        out.emit(item);
      }
    }

  }
}
