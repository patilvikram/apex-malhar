package org.apache.apex.python;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import py4j.GatewayConnection;
import py4j.GatewayServer;
import py4j.GatewayServerListener;

/**
 * Created by vikram on 2/3/17.
 */
public class JavaPy4jServerConnection implements GatewayServerListener
{
  private static final Logger LOG = LoggerFactory.getLogger(JavaPy4jServerConnection.class);

  private Process pythonProcess;
  private GatewayServer gatewayServer;

  public JavaPy4jServerConnection(Process pythonProcess, GatewayServer gatewayServer)
  {
    this.pythonProcess = pythonProcess;
    this.gatewayServer = gatewayServer;
  }

  @Override
  public void connectionError(Exception e)
  {

  }

  /**
   * This is for tracking python connection started or not
   *
   * @param gatewayConnection
   */
  @Override
  public void connectionStarted(GatewayConnection gatewayConnection)
  {
    LOG.debug(System.currentTimeMillis() + " Python Connection started" + gatewayConnection.getSocket().getPort());

  }

  //  This is for tracking python connection stopped
  @Override
  public void connectionStopped(GatewayConnection gatewayConnection)
  {
    LOG.debug(System.currentTimeMillis() + " Python Connection stopped" + gatewayConnection.getSocket().getPort());
  }

  // Exception ocurred on python side
  @Override
  public void serverError(Exception e)
  {
    LOG.debug(System.currentTimeMillis() + " Gateway Server exception reported");
    e.printStackTrace();
  }

  @Override
  public void serverPostShutdown()
  {
    LOG.debug(System.currentTimeMillis() + " Gateway Server already shutdown");
  }

  @Override
  public void serverPreShutdown()
  {
    LOG.debug(System.currentTimeMillis() + " Gateway Server shutting down now");
  }

  @Override
  public void serverStarted()
  {
    LOG.debug(System.currentTimeMillis() + " Gateway Connection started");
  }

  @Override
  public void serverStopped()
  {
    pythonProcess.destroy();

    LOG.debug(System.currentTimeMillis() + " Gateway Server stopped");
  }
}

