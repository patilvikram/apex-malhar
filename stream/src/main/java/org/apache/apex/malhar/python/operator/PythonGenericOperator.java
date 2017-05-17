package org.apache.apex.malhar.python.operator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.python.operator.runtime.PythonWorkerContext;
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

public abstract class PythonGenericOperator<T> extends BaseOperator
{

  protected transient GatewayServer server = null;
  protected transient PythonGatewayServerListenser py4jListener = null;
  protected transient PythonWorkerProxy<T> pythonWorkerProxy = null;
  protected transient Process pyProcess = null;
  protected byte[] serializedFunction = null;
  private static final Logger LOG = LoggerFactory.getLogger(PythonGenericOperator.class);
  protected transient OpType operationType = null;
  private PythonWorkerContext pythonWorkerContext = null;

  private Map<String, String> pythonOperatorEnv = new HashMap<String, String>();


  public enum OpType
  {
    MAP("MAP"),
    FLAT_MAP("FLAT_MAP"),
    FILTER("FILTER");

    private String operationName = null;

    OpType(String name)
    {
      this.operationName = name;
    }

    public String getType()
    {
      return operationName;
    }

  }

  public final transient DefaultInputPort<T> in = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {

      processTuple(tuple);

    }

  };
  public final transient DefaultOutputPort<T> out = new DefaultOutputPort<T>();

  public PythonGenericOperator()
  {
    this.serializedFunction = null;

  }

  public PythonGenericOperator(byte[] serializedFunc)
  {
    this.serializedFunction = serializedFunc;
  }

  public PythonGenericOperator(PythonWorkerContext context)
  {
    this.pythonWorkerContext = context;
  }

  public void setup(OperatorContext context)
  {
    AttributeMap attMap = context.getAttributes();
    Iterator hdfsPaths = attMap.entrySet().iterator();

    while (hdfsPaths.hasNext()) {
      Entry classpath = (Entry)hdfsPaths.next();
      LOG.debug(" Printing Entry " + ((Attribute)classpath.getKey()).getName() + " " + classpath.getValue());
    }
    this.pythonWorkerContext.setup();

    LOG.trace("APPLICATION PATH FROM PYTHON OPERATOR" + (String)context.getValue(DAGContext.APPLICATION_PATH));
    this.pythonWorkerProxy = new PythonWorkerProxy<>(this.serializedFunction);
    int port = NetworkUtils.findAvaliablePort();
    this.server = new GatewayServer(this.pythonWorkerProxy, port);
    this.py4jListener = new PythonGenericOperator.PythonGatewayServerListenser(this.server, this.pythonWorkerContext);
    this.server.addListener(this.py4jListener);
    this.server.start(true);

    int pythonServerStartAttempts = 5;
    while (!this.py4jListener.isPythonServerStarted() && !this.pythonWorkerProxy.isFunctionEnabled() && pythonServerStartAttempts > 0) {
      try {
        Thread.sleep(5000L);
        LOG.debug("WAITING FOR PYTHON WORKER REGISTRATION");
        --pythonServerStartAttempts;
      } catch (InterruptedException var9) {
        var9.printStackTrace();
      }
    }

    if (this.pythonWorkerProxy.isWorkerRegistered()) {
      this.pythonWorkerProxy.setFunction(this.operationType.getType());
    }
    if (!this.py4jListener.isPythonServerStarted()) {
      LOG.error("Python server could not be started");
    }

  }

  public void teardown()
  {
    if (server != null) {
      server.shutdown();
    }
  }

  public static class PythonGatewayServerListenser implements GatewayServerListener
  {

    private GatewayServer server = null;
    private Process pyProcess = null;
    private boolean pythonServerStarted = false;
    private OpType operationType = OpType.MAP;
    private static final Logger LOG = LoggerFactory.getLogger(PythonGatewayServerListenser.class);
    private PythonWorkerContext context = null;

    public boolean isPythonServerStarted()
    {
      return this.pythonServerStarted;
    }

    public PythonGatewayServerListenser(GatewayServer startedServer, PythonWorkerContext context)
    {
      this.server = startedServer;
      this.context = context;
    }

    public void connectionError(Exception e)
    {
      LOG.debug("Python Connection error :" + e.getMessage());

    }

    public void connectionStarted(GatewayConnection gatewayConnection)
    {
      LOG.debug("Python Connection started" + gatewayConnection.getSocket().getPort());

    }

    public void connectionStopped(GatewayConnection gatewayConnection)
    {
      LOG.debug("Python Connection stoppped");
    }

    public void serverError(Exception e)
    {
      LOG.debug("Gatewaye Server error" + e.getMessage());
    }

    public void serverPostShutdown()
    {

      LOG.debug("Gateway server shut down");
    }

    public void serverPreShutdown()
    {
      LOG.debug("Gateway server shutting down");

      if (this.pyProcess != null) {
        this.pyProcess.destroy();
        LOG.debug("Destroyed python worker process");
      }
    }

    public void serverStarted()
    {
      LOG.debug("Gateway server started:" + this.server.getPort());
      this.startPythonWorker(this.server.getPort());
    }

    public void serverStopped()
    {
      LOG.debug("Gateway server stopped");
      if (this.pyProcess != null) {
        this.pyProcess.destroy();
        LOG.debug("Destroyed python worker process");
      }

    }

    private void startPythonWorker(int gatewayServerPort)
    {
      ProcessBuilder pb = new ProcessBuilder(new String[0]);

      try {
        LOG.info("STARTING python worker process");
        LOG.info("Worker File Path : " + this.context.getWorkerFilePath());
        LOG.info("Python Environment Path : " + this.context.getPythonEnvPath());
        Map<String, String> processEnvironment = pb.environment();
        processEnvironment.put("PYTHONPATH", this.context.getPythonEnvPath());
        this.pyProcess = pb.command(new String[]{"/usr/bin/python", "-u", this.context.getWorkerFilePath(), "" + gatewayServerPort, operationType.getType()}).start();
        LoggerUtils.captureProcessStreams(this.pyProcess);
        this.pythonServerStarted = true;
        PythonGenericOperator.LOG.info("Python worker started " + this.pyProcess);
      } catch (IOException var8) {
        var8.printStackTrace();
        PythonGenericOperator.LOG.error("FAILED TO START PYTHON SERVER");
      }

    }
  }

  protected abstract void processTuple(T tuple);

  public void setPythonOperatorEnv(Map<String, String> pythonOperatorEnv)
  {
    this.pythonOperatorEnv = pythonOperatorEnv;
  }

}
