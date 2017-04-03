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
package org.apache.apex.malhar.python.operator.runtime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by vikram on 3/3/17.
 */
public class PythonWorkerProxy<T>
{
  private static final Logger LOG = LoggerFactory.getLogger(PythonWorkerProxy.class);
  private PythonWorker registerdPythonWorker = null;
  private boolean functionEnabled = false;
  private byte[] serializedFunction = null;
  private boolean workerRegistered = false;

  public PythonWorkerProxy(byte[] serializedFunc)
  {
    this.serializedFunction = serializedFunc;
  }

  public Object execute(T tuple)
  {
    if (registerdPythonWorker != null) {
      setFunction();
      LOG.info("processing tuple " + tuple);

      Object result = registerdPythonWorker.execute(tuple);
      LOG.info("processed tuple " + result);
      return result;

    }
    return null;
  }

  public void register(PythonWorker pythonWorker)
  {
    LOG.info("Registering python worker now ");
    this.registerdPythonWorker = pythonWorker;
    this.workerRegistered = true;
    LOG.info("Python worker registered ");
  }

  public void setFunction()
  {
    if (this.isWorkerRegistered() && !isFunctionEnabled()) {
      LOG.info("Setting SERIALIZED FUNCTION");
      this.registerdPythonWorker.setFunction(this.serializedFunction);
      this.functionEnabled = true;
      LOG.info("Set SERIALIZED FUNCTION");
    }
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
