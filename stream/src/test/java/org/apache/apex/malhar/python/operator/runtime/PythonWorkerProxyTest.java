package org.apache.apex.malhar.python.operator.runtime;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import py4j.Py4JException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({PythonWorkerProxy.class, LoggerFactory.class})
public class PythonWorkerProxyTest
{

  public static class PythonTestWorker implements PythonWorker
  {

    @Override
    public Object setFunction(byte[] func, String opType)
    {
      return opType;
    }

    @Override
    public Object execute(Object tuple)
    {
      return tuple;
    }
  }

  @Test
  public void testPythonWorkerRegisterAndExecute()
  {
    mockStatic(LoggerFactory.class);
    Logger logger = mock(Logger.class);
    when(LoggerFactory.getLogger(PythonWorkerProxy.class)).thenReturn(logger);

    String functionData = new String("TestFunction");
    PythonWorkerProxy workerProxy = new PythonWorkerProxy(functionData.getBytes());
    PythonTestWorker worker = new PythonTestWorker();
    workerProxy.register(worker);
    workerProxy.setFunction("DUMMY_OPERATION");
    Assert.assertEquals("TUPLE", worker.execute("TUPLE"));
  }

  @Test()
  public void testPythonFailureWhileProcessingTuple()
  {
    mockStatic(LoggerFactory.class);
    Logger logger = mock(Logger.class);
    when(LoggerFactory.getLogger(any(Class.class))).thenReturn(logger);

    String exceptionString = "DUMMY EXCEPTION";
    String functionData = new String("TestFunction");
    PythonWorker failingMockWorker = mock(PythonWorker.class);
    when(failingMockWorker.execute("TUPLE")).thenThrow(new Py4JException(exceptionString));

    PythonWorkerProxy workerProxy = new PythonWorkerProxy(functionData.getBytes());
    workerProxy.register(failingMockWorker);
    verify(logger).debug("Registering python worker now");
    verify(logger).debug("Python worker registered");
    String tupleValue = "TUPLE";
    Assert.assertEquals(null, workerProxy.execute(tupleValue));

    verify(logger).trace("Processing tuple:" + tupleValue);
    verify(logger).error("Exception encountered while executing operation for tuple:" + tupleValue + " Message:" + exceptionString);

  }
}
