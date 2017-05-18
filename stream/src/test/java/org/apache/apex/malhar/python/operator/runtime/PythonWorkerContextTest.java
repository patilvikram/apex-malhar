package org.apache.apex.malhar.python.operator.runtime;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.python.operator.PythonGenericOperator;

/**
 * Created by vikram on 17/5/17.
 */
public class PythonWorkerContextTest
{

  @Test
  public void testPythonWorkerContextTest()
  {
    PythonWorkerContext context = new PythonWorkerContext();
    String currentWorkingDirectory = System.getProperty("user.dir");
    Assert.assertEquals(currentWorkingDirectory + "/./" + PythonWorkerContext.PY4J_SRC_ZIP_FILE_NAME, context.getPythonEnvPath());
    Assert.assertEquals(currentWorkingDirectory + "/./" + PythonWorkerContext.PYTHON_WORKER_FILE_NAME, context.getWorkerFilePath());
  }

  @Test
  public void testPythonWorkerContextWithEnvironmentTest()
  {

    Map<String, String> operatorEnv = new HashMap<>();
    String pythonDependencyPath = "/home/user/test/dependencypath";
    String pythonWorkerPath = "/home/user/test/workerPath";

    operatorEnv.put(PythonWorkerContext.PY4J_DEPENDENCY_PATH, pythonDependencyPath);
    operatorEnv.put(PythonWorkerContext.PYTHON_WORKER_PATH, pythonWorkerPath);
    byte[] serializedFunction = new byte[10];
    PythonWorkerContext context = new PythonWorkerContext(PythonGenericOperator.OpType.MAP, serializedFunction, operatorEnv);
    String currentWorkingDirectory = System.getProperty("user.dir");
    Assert.assertEquals(pythonDependencyPath, context.getDependencyPath());
    Assert.assertEquals(pythonWorkerPath, context.getWorkerFilePath());

  }


}
