package org.apache.apex.python;

/**
 * Created by vikram on 3/3/17.
 */
public interface PythonWorker<T>
{

  public void setFunction(Object func);

  public Object execute(T tuple);
}
