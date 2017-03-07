package org.apache.apex.python;

/**
 * Created by vikram on 2/3/17.
 */
public interface PythonFunctionExecutor
{
  void setFunc(byte[] func);

  void execute(Object object);

}
