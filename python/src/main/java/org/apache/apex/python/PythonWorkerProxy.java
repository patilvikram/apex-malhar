package org.apache.apex.python;

/**
 * Created by vikram on 3/3/17.
 */
public class PythonWorkerProxy<T>
{
  private PythonWorker registerdPythonWorker = null;

  public PythonWorkerProxy()
  {

  }

  public Object execute(T tuple)
  {
    if (registerdPythonWorker != null) {
      return registerdPythonWorker.execute(tuple);
    }
    return null;
  }

  public void register(PythonWorker pythonWorker)
  {
    this.registerdPythonWorker = pythonWorker;
  }
}
