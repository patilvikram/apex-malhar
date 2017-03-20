/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.stream.api.impl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.stream.api.python.PythonWorkerProxy;
import org.apache.apex.malhar.stream.api.util.NetworkUtils;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

import py4j.GatewayConnection;
import py4j.GatewayServer;
import py4j.GatewayServerListener;

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

    Attribute.AttributeMap attMap = context.getAttributes();
    for (Map.Entry<Attribute<?>, Object> entry : attMap.entrySet()) {
      LOG.info(" Printing Entry " + entry.getKey().getName() + " " + entry.getValue());
    }

    LOG.info("APPLICATION PATH FROM PYTHON OPERATOR" + context.getValue(DAG.DAGContext.APPLICATION_PATH));

    List<String> hdfsPaths = new ArrayList<String>();
    hdfsPaths.add(context.getValue(DAG.DAGContext.APPLICATION_PATH) + "/py4j-0.10.4.tar.gz");
    hdfsPaths.add(context.getValue(DAG.DAGContext.APPLICATION_PATH) + "/worker.py");
    // Add serrialized file path here and load locally

    // launch gateway server and python script here
    // from python register worker
    // register serialized function on PythonWorker

    String classpath = System.getProperty("java.class.path");
    String[] classpathEntries = classpath.split(File.pathSeparator);
    LOG.info("CLASSPATH" + classpath);
    pythonWorkerProxy = new PythonWorkerProxy<T>(this.serializedFunction);
    int port = NetworkUtils.findAvaliablePort();

    server = new GatewayServer(pythonWorkerProxy, port);
    LOG.info("Port number" + port);
    LOG.error("Port number" + port);
    py4jListener = new PythonGatewayServerListenser(server);
    server.addListener(py4jListener);
    server.start(true);

    int pythonServerStartAttempts = 5;
    while (py4jListener.isPythonServerStarted() && pythonServerStartAttempts > 0) {
      try {
        Thread.sleep(5000);
        pythonServerStartAttempts--;
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    if (py4jListener.isPythonServerStarted()) {
      LOG.error("Python server could not be started");
    }

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

    public boolean isPythonServerStarted()
    {
      return pythonServerStarted;
    }

    private boolean pythonServerStarted = false;

    public PythonGatewayServerListenser(GatewayServer startedServer)
    {

      this.server = startedServer;
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

      this.startPythonWorker(server.getPort());

    }

    @Override
    public void serverStopped()
    {
      LOG.info("Gateway server stopped");
    }

    private void startPythonWorker(int gatewayServerPort)
    {

      ProcessBuilder pb = new ProcessBuilder("/usr/bin/python", "/home/vikram/Documents/src/apex-malhar/python/scripts/OperatorWorker.py", "" + gatewayServerPort);
      try {
        LOG.info("STARTING python worker process");
        Map<String, String> pbEnv = pb.environment();
        pbEnv.put("PYTHONPATH", "/home/vikram/.local/lib/python2.7/site-packages");
        pb.inheritIO();
        this.pyProcess = pb.start();
        this.pythonServerStarted = true;
        LOG.info("Python worker started ");
      } catch (IOException e) {
        e.printStackTrace();
        LOG.error("FAILED TO START PYTHON SERVER");

      }

    }

  }

  private void copyFilesToLocalResource(List<String> hdfsFilePaths)
  {
    LOG.debug("Moving files locally ");
    for(String hdfsInputFilePath : hdfsFilePaths) {
      Path path = new Path(stringPath);
      FileSystem fs = FileSystem.get(path.toUri(), conf);
    }

  }

}
