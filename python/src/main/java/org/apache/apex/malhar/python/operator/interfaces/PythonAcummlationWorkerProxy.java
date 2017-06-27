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
package org.apache.apex.malhar.python.operator.interfaces;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.window.Accumulation;
import org.apache.apex.malhar.python.runtime.PythonAccumulatorWorker;
import org.apache.apex.malhar.python.runtime.PythonWorker;
import org.apache.apex.malhar.python.runtime.PythonWorkerProxy;

import py4j.Py4JException;

public class PythonAcummlationWorkerProxy<T> extends PythonWorkerProxy<T> implements Accumulation<T,T,T>
{
  private static final Logger LOG = LoggerFactory.getLogger(PythonAcummlationWorkerProxy.class);
  private PythonAccumulatorWorker<T> registerdPythonWorker = null;
  private boolean functionEnabled = false;
  private byte[]  serializedObject = null;
  private boolean workerRegistered = false;

  public PythonAcummlationWorkerProxy(byte[] serializedFunc)
  {
    super(serializedFunc);
    this.serializedObject = serializedFunc;
  }


  public void register(PythonAccumulatorWorker pythonWorker)
  {
    LOG.debug("Registering python worker now");
    this.registerdPythonWorker = pythonWorker;
    this.workerRegistered = true;
    LOG.debug("Python worker registered");
  }

  public void setObject(String opType)
  {
    if (this.isWorkerRegistered() && !isFunctionEnabled()) {
      LOG.debug("Setting Serialized function");
      this.registerdPythonWorker.setObject(this.serializedObject, opType);
      this.functionEnabled = true;
      LOG.debug("Set Serialized function");
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

  @Override
  public T defaultAccumulatedValue()
  {
    return null;
  }

  @Override
  public T accumulate(T accumulatedValue, T input)
  {
    if (registerdPythonWorker != null) {

      T result = null;
      LOG.trace("Processing accumulation: {}", input);
      try {
        result = registerdPythonWorker.accumulate(accumulatedValue, input);
        LOG.trace("Processed accumulation: {}", result);
        return result;
      } catch (Py4JException ex) {
        LOG.error("Exception encountered while executing operation for tuple: {}  Message: {}", input, ex.getMessage());
      } finally {
        return null;
      }
    }
    return null;

  }

  @Override
  public T merge(T accumulatedValue1, T accumulatedValue2)
  {
    if (registerdPythonWorker != null) {

      T result = null;
      LOG.trace("Processing accumulation: {} {}", accumulatedValue1, accumulatedValue2);
      try {
        result = registerdPythonWorker.merge(accumulatedValue1, accumulatedValue2);
        LOG.trace("Processed accumulation: {}", result);
        return result;
      } catch (Py4JException ex) {
        LOG.error("Exception encountered while executing operation for accumulation: {} {}  Message: {}",accumulatedValue1, accumulatedValue2, ex.getMessage());
      } finally {
        return null;
      }
    }
    return null;

  }

  @Override
  public T getOutput(T accumulatedValue)
  {
    return null;
  }

  @Override
  public T getRetraction(T value)
  {
    return null;
  }
}
