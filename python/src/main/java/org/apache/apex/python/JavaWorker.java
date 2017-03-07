package org.apache.apex.python;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import py4j.GatewayServer;

/**
 * Created by vikram on 1/3/17.
 */
public class JavaWorker implements Runnable
{
  private Adder adder = null;

  private int gatewayPort = -1;
  private static final Logger LOG = LoggerFactory.getLogger(JavaWorker.class);
  public synchronized boolean isGatewayServerStatus()
  {
    return gatewayServerStatus;
  }

  public synchronized void setGatewayServerStatus(boolean gatewayServerStatus)
  {
    this.gatewayServerStatus = gatewayServerStatus;
  }

  private boolean gatewayServerStatus;

  public Process getPythonProcess()
  {
    return pythonProcess;
  }

  public void setPythonProcess(Process pythonProcess)
  {
    this.pythonProcess = pythonProcess;
  }

  private Process pythonProcess = null;

  public GatewayServer getGatewayServer()
  {
    return gatewayServer;
  }

  public void setGatewayServer(GatewayServer gs)
  {
    this.gatewayServer = gs;
  }

  private GatewayServer gatewayServer = null;

  public JavaWorker()
  {

    LOG.debug("Registered Gateway Server ");
  }

  public synchronized void register(Adder newAdder)
  {
//   Log.debug(" Registering "+ newAdder.getName());
    if (this.adder == null) {

      this.adder = newAdder;
     LOG.debug(" Adder implementation registered");
    }
  }

  public boolean isRegistered()
  {
    return adder != null;
  }

  public Adder getRegistered()
  {
    return adder;
  }

  public int getGatewayPort()
  {
    return gatewayPort;
  }

  public void setGatewayPort(int gatewayPort)
  {
    this.gatewayPort = gatewayPort;
  }

  public void spawnProcess()
  {

    ProcessBuilder pb =
      new ProcessBuilder("python", "/home/vikram/Documents/src/PyApex/worker.py", "" + this.getGatewayPort());

    pb.directory(new File("/home/vikram/Documents/src/PyApex/"));
    File log = new File("log");
    pb.redirectErrorStream(true);
    pb.redirectOutput(ProcessBuilder.Redirect.appendTo(log));
    try {
      this.pythonProcess = pb.start();
     LOG.debug("Spawned python process");
    } catch (IOException e) {
      e.printStackTrace();

    }
  }

  @Override
  public void run()
  {

    while (this.isGatewayServerStatus()) {
      // check if python process is spawned

      // check if gateway server is still running if its not kill python process as well.
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

  }

  boolean isRunning(Process process)
  {
    try {
      process.exitValue();
      return false;
    } catch (Exception e) {
      return true;
    }
  }

  public void shutdown()
  {
    gatewayServer.shutdown();
  }

}
