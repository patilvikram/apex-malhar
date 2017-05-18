package org.apache.apex.malhar.python.operator.runtime;

import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.python.operator.PythonGenericOperator;

public class PythonWorkerContext implements Serializable
{
  public static String PY4J_DEPENDENCY_PATH = "PY4J_DEPENDENCY_PATH";
  public static String PYTHON_WORKER_PATH = "PYTHON_WORKER_PATH";
  public static String PY4J_SRC_ZIP_FILE_NAME = "py4j-0.10.4-src.zip";
  public static String PYTHON_WORKER_FILE_NAME = "worker.py";
  public static String ENV_VAR_PYTHONPATH = "PYTHONPATH";

  private String dependencyPath = null;
  private String workerFilePath = null;
  private String pythonEnvPath = null;
  private static final Logger LOG = LoggerFactory.getLogger(PythonWorkerContext.class);

  private byte[] serializedFunction = null;
  private PythonGenericOperator.OpType opType = null;

  private Map<String, String> environmentData = new HashMap<String, String>();

  public PythonWorkerContext()
  {

  }

  public PythonWorkerContext(PythonGenericOperator.OpType operationType, byte[] serializedFunction, Map<String, String> environmentData)
  {
    this();
    this.opType = operationType;
    this.serializedFunction = serializedFunction;
    this.environmentData = environmentData;
  }

  public void setup()
  {
    LOG.info("Setting up worker context: " + this);
    File py4jDependencyFile = new File("./" + PY4J_SRC_ZIP_FILE_NAME);
    pythonEnvPath = System.getenv(ENV_VAR_PYTHONPATH);
    LOG.info("PYTHON PATH: " + pythonEnvPath);
    if (pythonEnvPath != null) {
      pythonEnvPath = py4jDependencyFile.getAbsolutePath() + ":" + pythonEnvPath;
    } else {
      pythonEnvPath = py4jDependencyFile.getAbsolutePath();
    }
    LOG.info("FINAL PYTHON PATH: " + pythonEnvPath);
    LOG.info("FINAL DEPENDENCY PATH: " + environmentData.get(PY4J_DEPENDENCY_PATH));
    if ((this.dependencyPath = environmentData.get(PY4J_DEPENDENCY_PATH)) == null) {
      this.dependencyPath = py4jDependencyFile.getAbsolutePath();
    }

    LOG.info("FINAL WORKER PATH: " + environmentData.get(PYTHON_WORKER_PATH));
    if ((this.workerFilePath = environmentData.get(PYTHON_WORKER_PATH)) == null) {
      File pythonWorkerFile = new File("./" + PYTHON_WORKER_FILE_NAME);
      this.workerFilePath = pythonWorkerFile.getAbsolutePath();
    }

    LOG.info("Python dependency Path " + this.dependencyPath + " worker Path " + this.workerFilePath);
  }

  public synchronized String getDependencyPath()
  {
    return this.dependencyPath;
  }

  public synchronized String getWorkerFilePath()
  {
    return this.workerFilePath;
  }

  public synchronized String getPythonEnvPath()
  {
    return this.pythonEnvPath;
  }

  public synchronized  byte[] getSerializedFunction()
  {
    return this.serializedFunction;
  }


  public synchronized Map<String, String> getEnvironmentData()
  {
    return this.environmentData;
  }

  public synchronized void setEnvironmentData(Map<String, String> environmentData)
  {
    this.environmentData = environmentData;
  }
}
