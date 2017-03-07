package org.apache.apex.malhar.stream.api.python;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by vikram on 3/3/17.
 */
public class PythonWorkerProxy<T>
{
  private static final Logger LOG = LoggerFactory.getLogger(PythonWorkerProxy.class);
  private PythonWorker registerdPythonWorker = null;
  private byte[] serializedFunction = null;

  public PythonWorkerProxy(byte[] serializedFunc)
  {
    this.serializedFunction = serializedFunc;
  }

  public Object execute(T tuple)
  {
    if (registerdPythonWorker != null) {

      LOG.info("processing tuple " + tuple);

      Object result = registerdPythonWorker.execute(tuple);
      LOG.info("processed tuple " + result);
      return result;

    }
    return null;
  }

  public void register(PythonWorker pythonWorker)
  {
    LOG.info("Registering python worker ");
    this.registerdPythonWorker = pythonWorker;
    this.registerdPythonWorker.setFunction(this.serializedFunction);
  }
}
