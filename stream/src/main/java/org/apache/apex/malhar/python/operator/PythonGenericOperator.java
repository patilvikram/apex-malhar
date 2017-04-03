//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package org.apache.apex.malhar.python.operator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.python.operator.runtime.PythonWorkerProxy;
import org.apache.apex.malhar.stream.api.util.LoggerUtils;
import org.apache.apex.malhar.stream.api.util.NetworkUtils;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

import py4j.GatewayConnection;
import py4j.GatewayServer;
import py4j.GatewayServerListener;

public class PythonGenericOperator<T> extends BaseOperator
{
  private transient GatewayServer server = null;
  private transient PythonGenericOperator.PythonGatewayServerListenser py4jListener = null;
  private transient PythonWorkerProxy<T> pythonWorkerProxy = null;
  private transient Process pyProcess = null;
  private byte[] serializedFunction = null;
  private static final Logger LOG = LoggerFactory.getLogger(PythonGenericOperator.class);

  public final transient DefaultInputPort<T> in = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      PythonGenericOperator.LOG.info("Received Tuple " + tuple);
      Object result = pythonWorkerProxy.execute(tuple);
      PythonGenericOperator.this.out.emit((String)result);
    }

  };
  public final transient DefaultOutputPort<String> out = new DefaultOutputPort();

  public PythonGenericOperator()
  {
    this.serializedFunction = null;
  }

  public PythonGenericOperator(byte[] serializedFunc)
  {
    this.serializedFunction = serializedFunc;
  }

  public void setup(OperatorContext context)
  {
    AttributeMap attMap = context.getAttributes();
    Iterator hdfsPaths = attMap.entrySet().iterator();

    while (hdfsPaths.hasNext()) {
      Entry classpath = (Entry)hdfsPaths.next();
      LOG.info(" Printing Entry " + ((Attribute)classpath.getKey()).getName() + " " + classpath.getValue());
    }

    LOG.info("APPLICATION PATH FROM PYTHON OPERATOR" + (String)context.getValue(DAGContext.APPLICATION_PATH));
    ArrayList var10 = new ArrayList();
    var10.add((String)context.getValue(DAGContext.APPLICATION_PATH) + "/py4j-0.10.4-src.zip");
    var10.add((String)context.getValue(DAGContext.APPLICATION_PATH) + "/worker.py");
    String var11 = System.getProperty("java.class.path");
    String[] classpathEntries = var11.split(File.pathSeparator);
    LOG.info("CLASSPATH" + var11);
    this.pythonWorkerProxy = new PythonWorkerProxy(this.serializedFunction);
    int port = NetworkUtils.findAvaliablePort();
    this.server = new GatewayServer(this.pythonWorkerProxy, port);
    LOG.info("Port number" + port);
    LOG.error("Port number" + port);
    this.py4jListener = new PythonGenericOperator.PythonGatewayServerListenser(this.server);
    this.server.addListener(this.py4jListener);
    this.server.start(true);
    int pythonServerStartAttempts = 5;
    String currentUserName = System.getProperty("user.name");
    LOG.info("Port number" + currentUserName);
    while (!this.py4jListener.isPythonServerStarted() && !this.pythonWorkerProxy.isFunctionEnabled() && pythonServerStartAttempts > 0) {
      try {
        Thread.sleep(5000L);
        LOG.info("WAITING FOR PYTHON WORKER REGISTRATION");
        --pythonServerStartAttempts;
      } catch (InterruptedException var9) {
        var9.printStackTrace();
      }
    }

    if (this.pythonWorkerProxy.isWorkerRegistered()) {
      this.pythonWorkerProxy.setFunction();
    }
    if (!this.py4jListener.isPythonServerStarted()) {
      LOG.error("Python server could not be started");
    }

  }

  public void teardown()
  {
  }

  private void copyFilesToLocalResource(List<String> hdfsFilePaths)
  {
    LOG.debug("Moving files locally ");
  }

  public static class PythonGatewayServerListenser implements GatewayServerListener
  {
    private String py4jSrcZip = "py4j-0.10.4-src.zip";
    private GatewayServer server = null;
    private Process pyProcess = null;
    private boolean pythonServerStarted = false;
    private static final Logger LOG = LoggerFactory.getLogger(PythonGatewayServerListenser.class);

    public boolean isPythonServerStarted()
    {
      return this.pythonServerStarted;
    }

    public PythonGatewayServerListenser(GatewayServer startedServer)
    {
      this.server = startedServer;
    }

    public void connectionError(Exception e)
    {
      LOG.info("Python Connection error :" + e.getMessage());

    }

    public void connectionStarted(GatewayConnection gatewayConnection)
    {
      LOG.info("Python Connection started");

    }

    public void connectionStopped(GatewayConnection gatewayConnection)
    {
      LOG.info("Python Connection stoppped");
    }

    public void serverError(Exception e)
    {
      LOG.info("Gatewaye Server error" + e.getMessage());
    }

    public void serverPostShutdown()
    {
      LOG.info("Gateway server shut down");
    }

    public void serverPreShutdown()
    {
      LOG.info("Gateway server shutting down");
    }

    public void serverStarted()
    {
      LOG.info("Gateway server started");
      this.startPythonWorker(this.server.getPort());
    }

    public void serverStopped()
    {
      PythonGenericOperator.LOG.info("Gateway server stopped");
      if (this.pyProcess != null) {
        this.pyProcess.destroy();
        LOG.info("Destroyed python worker process");
      }

    }

    private void startPythonWorker(int gatewayServerPort)
    {
      ProcessBuilder pb = new ProcessBuilder(new String[0]);
      String PYTHONPATH = System.getenv("PYTHONPATH");
      LOG.info("Existing PYTHON PATH" + PYTHONPATH);

      try {
        PythonGenericOperator.LOG.info("STARTING python worker process");

        Map e = pb.environment();
        File py4jDependencyFile = new File("./" + py4jSrcZip);
        if (PYTHONPATH != null) {
          PYTHONPATH = py4jDependencyFile.getAbsolutePath() + ":" + PYTHONPATH;
        } else {
          PYTHONPATH = py4jDependencyFile.getAbsolutePath();
        }
        LOG.info("FINALE PYTHON PATH" + PYTHONPATH);
        String py4jDependencyePath = py4jDependencyFile.getAbsolutePath();
        if (py4jDependencyFile.exists()) {
          LOG.info(" " + py4jDependencyePath + " Exists ");

        }
        File pythonWorkerFile = new File("./worker.py");
        String py4jWorkerPath = pythonWorkerFile.getAbsolutePath();
        PythonGenericOperator.LOG.info("Python dependency Path " + py4jDependencyePath + " worker Path " + py4jWorkerPath);
        e.put("PYTHONPATH", PYTHONPATH);
        this.pyProcess = pb.command(new String[]{"/usr/bin/python", "-u", py4jWorkerPath, "" + gatewayServerPort}).start();
        LoggerUtils.captureProcessStreams(this.pyProcess);

        this.pythonServerStarted = true;
        PythonGenericOperator.LOG.info("Python worker started " + this.pyProcess);
      } catch (IOException var8) {
        var8.printStackTrace();
        PythonGenericOperator.LOG.error("FAILED TO START PYTHON SERVER");
      }

    }
  }
}
