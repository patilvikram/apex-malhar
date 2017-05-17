package org.apache.apex.malhar.python.operator.runtime;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.python.operator.PythonGenericOperator;

public class PythonWorkerContext
{
  private static String PY4J_SRC_ZIP_FILE_NAME = "py4j-0.10.4-src.zip";
  private static String PYTHON_WORKER_FILE_NAME = "worker.py";
  private String dependencyPath = null;
  private String workerFilePath = null;
  private String pythonEnvPath = null;
  private static final Logger LOG = LoggerFactory.getLogger(PythonGenericOperator.class);

  private byte[] serializedFunction = null;
  private PythonGenericOperator.OpType opType = null;
  private Map<String, String> pythonOperatorEnv = new HashMap<String, String>();

  public PythonWorkerContext()
  {

  }

  public PythonWorkerContext(PythonGenericOperator.OpType operationType, byte[] serializedFunction)
  {
    this.setup();
    this.opType = operationType;
    this.serializedFunction = serializedFunction;
  }

  public void setup()
  {
    String PYTHONPATH = System.getenv("PYTHONPATH");
    LOG.info("PYTHON PATH" + PYTHONPATH);

    File py4jDependencyFile = new File("./" + PY4J_SRC_ZIP_FILE_NAME);
    if (pythonEnvPath != null) {
      pythonEnvPath = py4jDependencyFile.getAbsolutePath() + ":" + pythonEnvPath;
    } else {
      pythonEnvPath = py4jDependencyFile.getAbsolutePath();
    }
    LOG.info("FINAL PYTHON PATH" + pythonEnvPath);
    if ((dependencyPath = pythonOperatorEnv.get("PYTHON_DEPENDENCY_PATH")) == null) {
      dependencyPath = py4jDependencyFile.getAbsolutePath();
    }

    if ((workerFilePath = pythonOperatorEnv.get("PYTHON_WORKER_PATH")) == null) {
      File pythonWorkerFile = new File("./" + PYTHON_WORKER_FILE_NAME);
      workerFilePath = pythonWorkerFile.getAbsolutePath();
    }

    LOG.info("Python dependency Path " + dependencyPath + " worker Path " + workerFilePath);
  }

  public String getDependencyPath()
  {
    return dependencyPath;
  }

  public void setDependencyPath(String dependencyPath)
  {
    this.dependencyPath = dependencyPath;
  }

  public String getWorkerFilePath()
  {
    return workerFilePath;
  }

  public void setWorkerFilePath(String workerFilePath)
  {
    this.workerFilePath = workerFilePath;
  }

  public String getPythonEnvPath()
  {
    return pythonEnvPath;
  }

  public void setPythonEnvPath(String pythonEnvPath)
  {
    this.pythonEnvPath = pythonEnvPath;
  }

  public byte[] getSerializedFunction()
  {
    return serializedFunction;
  }

}
