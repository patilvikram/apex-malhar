/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.python.operator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.python.runtime.PythonWorkerContext;
import org.apache.apex.malhar.python.runtime.PythonWorkerProxy;
import org.apache.apex.malhar.python.util.LoggerUtils;
import org.apache.apex.malhar.python.util.NetworkUtils;

import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

import py4j.GatewayConnection;
import py4j.GatewayServer;
import py4j.GatewayServerListener;
import py4j.Py4JServerConnection;

public abstract class PythonGenericOperator<T> extends BaseOperator
{

  protected transient GatewayServer server = null;
  protected transient PythonGatewayServerListenser py4jListener = null;
  protected transient PythonWorkerProxy<T> pythonWorkerProxy = null;
  protected byte[] serializedFunction = null;
  private static final Logger LOG = LoggerFactory.getLogger(PythonGenericOperator.class);
  protected transient OpType operationType = null;
  private PythonWorkerContext pythonWorkerContext = null;
  protected Map<String, String> environementData = new HashMap<String, String>();

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
    this(null);

  }

  public PythonGenericOperator(byte[] serializedFunc)
  {
    this.serializedFunction = serializedFunc;
    this.pythonWorkerContext = new PythonWorkerContext(this.operationType, serializedFunc, environementData);

  }

  public void setup(OperatorContext context)
  {
    LOG.debug("Application path from Python Operator: {} ", (String)context.getValue(DAGContext.APPLICATION_PATH));
    // Setting up context path explicitly for handling local as well Hadoop Based Application Development
    this.pythonWorkerContext.setup();

    this.pythonWorkerProxy = new PythonWorkerProxy<>(this.serializedFunction);

    // Instantiating Py4j Gateway Server for Python Worker Process connect back
    boolean gatewayServerLaunchSuccess = false;
    int serverStartAttempts = 5;
    while (!gatewayServerLaunchSuccess && serverStartAttempts > 0) {
      try {
        this.server = new GatewayServer(this.pythonWorkerProxy, NetworkUtils.findAvaliablePort());
        this.py4jListener = new PythonGenericOperator.PythonGatewayServerListenser(this.server, this.pythonWorkerContext);
        this.server.addListener(this.py4jListener);
        this.server.start(true);
        gatewayServerLaunchSuccess = true;
        --serverStartAttempts;
      } catch (Exception ex) {
        LOG.error("Gateway server failed to launch to due: {}" + ex.getMessage());
        gatewayServerLaunchSuccess = false;
      }
    }

    if (!gatewayServerLaunchSuccess) {
      throw new RuntimeException("Failed to launch Gateway Server");
    }

    serverStartAttempts = 5;

    while (!this.py4jListener.isPythonServerStarted() && !this.pythonWorkerProxy.isFunctionEnabled() && serverStartAttempts > 0) {
      try {
        Thread.sleep(5000L);
        LOG.debug("Waiting for Python Worker Registration");
        --serverStartAttempts;
      } catch (InterruptedException ex) {
        LOG.error("Python Callback server failed to launch to due: {}" + ex.getMessage());
      }
    }
    if (!pythonWorkerProxy.isWorkerRegistered()) {
      this.server.shutdown();
      throw new RuntimeException("Failed to launch Call Back Server");
    }

    // Transferring serialized function to Python Worker.
    if (this.pythonWorkerProxy.isWorkerRegistered()) {
      this.pythonWorkerProxy.setFunction(this.operationType.getType());
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
      LOG.debug("Python Connection error : {}", e.getMessage());

    }

    @Override
    public void connectionStarted(Py4JServerConnection py4JServerConnection)
    {

    }

    @Override
    public void connectionStopped(Py4JServerConnection py4JServerConnection)
    {

    }

    public void connectionStarted(GatewayConnection gatewayConnection)
    {
      LOG.debug("Python Connection started: {}", gatewayConnection.getSocket().getPort());

    }

    public void connectionStopped(GatewayConnection gatewayConnection)
    {
      LOG.debug("Python Connection stoppped");
    }

    public void serverError(Exception e)
    {
      LOG.debug("Gateway Server error: {}", e.getMessage());
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
      LOG.debug("Gateway server started: {}", this.server.getPort());
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
        LOG.info("Starting python worker process using context: {}", this.context);
        LOG.info("Worker File Path: {}", this.context.getWorkerFilePath());
        LOG.info("Python Environment Path: {}", this.context.getPythonEnvPath());
        Map<String, String> processEnvironment = pb.environment();
        processEnvironment.put("PYTHONPATH", this.context.getPythonEnvPath());
        this.pyProcess = pb.command(new String[]{"/usr/bin/python", "-u", this.context.getWorkerFilePath(), "" + gatewayServerPort, operationType.getType()}).start();
        LoggerUtils.captureProcessStreams(this.pyProcess);
        this.pythonServerStarted = true;
        PythonGenericOperator.LOG.info("Python worker started: {}", this.pyProcess);
      } catch (IOException exception) {

        PythonGenericOperator.LOG.error("Failed to start python server: {}" + exception.getMessage());
      }
    }
  }

  protected abstract void processTuple(T tuple);

  public void setPythonOperatorEnv(Map<String, String> environementData)
  {
    this.environementData = environementData;
    if (pythonWorkerContext == null) {
      this.pythonWorkerContext = new PythonWorkerContext(this.operationType, serializedFunction, environementData);
    } else {
      this.pythonWorkerContext.setEnvironmentData(environementData);
    }
  }

}
