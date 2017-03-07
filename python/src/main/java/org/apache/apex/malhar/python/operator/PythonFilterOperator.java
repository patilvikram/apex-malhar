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
package org.apache.apex.malhar.python.operator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultOutputPort;

public class PythonFilterOperator<T> extends PythonGenericOperator<T>
{
  private static final Logger LOG = LoggerFactory.getLogger(PythonFilterOperator.class);

  DefaultOutputPort<T> falsePort = new DefaultOutputPort<>();
  DefaultOutputPort<T> truePort = new DefaultOutputPort<T>();

  public PythonFilterOperator()
  {
    operationType = OpType.FILTER;
  }

  public PythonFilterOperator(byte[] serializedFunc)
  {
    super(serializedFunc);
    operationType = OpType.FILTER;
  }

  @Override
  protected void processTuple(T tuple)
  {
    LOG.trace("Received Tuple: {}", tuple);
    Object result = pythonWorkerProxy.execute(tuple);
    if (result instanceof Boolean) {
      Boolean b = (Boolean)result;
      LOG.trace("Filter response received: {}" + b);
      if (b.booleanValue()) {
        out.emit(tuple);
      }
    }

  }
}
