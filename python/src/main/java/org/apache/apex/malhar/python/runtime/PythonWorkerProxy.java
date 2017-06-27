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
package org.apache.apex.malhar.python.runtime;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py4j.Py4JException;

public class PythonWorkerProxy<T>
{
  private static final Logger LOG = LoggerFactory.getLogger(PythonWorkerProxy.class);


  private PythonWorker worker = null;
  private boolean functionEnabled = false;
  private byte[] serializedData = null;
  private boolean workerRegistered = false;

  public PythonWorkerProxy(byte[] serializedFunc)
  {
    this.serializedData = serializedFunc;
  }

  public Object execute(T tuple)
  {
    if (worker != null) {

      Object result = null;
      LOG.trace("Processing tuple: {}", tuple);
      try {
        result = worker.execute(tuple);
        LOG.trace("Processed tuple: {}", result);
        return result;
      } catch (Py4JException ex) {
        LOG.error("Exception encountered while executing operation for tuple: {}  Message: {}", tuple, ex.getMessage());
      } finally {

      }
    }
    return null;

  }

  public void register(PythonWorker pythonWorker)
  {
    LOG.debug("Registering python worker now");
    this.worker = pythonWorker;
    this.workerRegistered = true;
    LOG.debug("Python worker registered");
  }

  public void setSerializedData(String opType)
  {
    if (this.isWorkerRegistered() && !isFunctionEnabled()) {
      LOG.debug("Setting Serialized function");
      this.worker.setFunction(this.serializedData, opType);
      this.functionEnabled = true;
      LOG.debug("Set Serialized function");
    }
  }

  public byte[] getSerializedData(String opType)
  {
   return serializedData;
  }

  public PythonWorker getWorker()
  {
    return worker;
  }


  public boolean isWorkerRegistered()
  {
    return this.workerRegistered;
  }

  public boolean isFunctionEnabled()
  {
    return this.functionEnabled;
  }
}
