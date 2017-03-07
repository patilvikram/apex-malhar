package org.apache.apex.malhar.stream.api.impl;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.stream.api.python.PythonWorkerProxy;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

import py4j.GatewayConnection;
import py4j.GatewayServer;
import py4j.GatewayServerListener;

/**
 * Created by vikram on 2/3/17.
 */
public class PythonGenericOperator<T> extends BaseOperator
{

  private transient GatewayServer server = null;
  private transient PythonGatewayServerListenser py4jListener = null;
  private transient PythonWorkerProxy<T> pythonWorkerProxy = null;
  private transient Process pyProcess = null;
  private byte[] serializedFunction = null;

  private static final Logger LOG = LoggerFactory.getLogger(PythonGenericOperator.class);

  public PythonGenericOperator()
  {
    this.serializedFunction = null;
  }

  public PythonGenericOperator(byte[] serializedFunc)
  {
    this.serializedFunction = serializedFunc;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    // launch gateway server and python script here
    // from python register worker
    // register serialized function on PythonWorker

    pythonWorkerProxy = new PythonWorkerProxy<T>(this.serializedFunction);
    int port = 45454;
    server = new GatewayServer(pythonWorkerProxy, port);
    LOG.info("Port number" + port);
    LOG.error("Port number" + port);
    py4jListener = new PythonGatewayServerListenser(server, this.pyProcess);
    server.addListener(py4jListener);
    server.start(true);

//    this.startPythonWorker(server.getPort());

  }

  @Override
  public void teardown()
  {
    // check process
    // shutdown process

  }

  public final transient DefaultInputPort<T> in = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      LOG.info("Received Tuple " + tuple);
      Object result = pythonWorkerProxy.execute(tuple);
      out.emit((String)result);

    }
  };

  public final transient DefaultOutputPort<String> out = new DefaultOutputPort<>();

  public static class PythonGatewayServerListenser implements GatewayServerListener
  {
    private GatewayServer server = null;
    private Process pyProcess = null;

    public PythonGatewayServerListenser(GatewayServer startedServer, Process pyProcess)
    {

      this.server = startedServer;
      this.pyProcess = pyProcess;

    }

    @Override
    public void connectionError(Exception e)
    {

    }

    @Override
    public void connectionStarted(GatewayConnection gatewayConnection)
    {
      LOG.info("Connection started");
    }

    @Override
    public void connectionStopped(GatewayConnection gatewayConnection)
    {
      LOG.info("Connection stoppped");
    }

    @Override
    public void serverError(Exception e)
    {

    }

    @Override
    public void serverPostShutdown()
    {
      LOG.info("Gateway server shut down");
    }

    @Override
    public void serverPreShutdown()
    {
      LOG.info("Gateway server shutting down");
    }

    @Override
    public void serverStarted()
    {
      LOG.info("Gateway server started");
    }

    @Override
    public void serverStopped()
    {
      LOG.info("Gateway server stopped");
    }
  }

  private void startPythonWorker(int gatewayServerPort)
  {

    ProcessBuilder pb = new ProcessBuilder("python", "/home/vikram/Documents/src/PyApex/OperatorWorker.py", "" + gatewayServerPort);
    try {
      LOG.info("STARTING python worker process");
      this.pyProcess = pb.start();

      LOG.info("Python worker started  Exit Value: ");
//        return this.pyProcess;
    } catch (IOException e) {
      e.printStackTrace();
      LOG.error("FAILED TO START PYTHON SERVER");

    }

  }
}
