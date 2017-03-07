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
package org.apache.apex.python;

import com.datatorrent.stram.client.StramAppLauncher;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;

/**
 * Created by vikram on 22/2/17.
 */
public class PythonAppFactory implements StramAppLauncher.AppFactory
{
  private PythonApp streamingApp;
  private String name;

  public PythonAppFactory(String name, PythonApp app)
  {
    this.name = name;
    this.streamingApp = app;
  }

  public LogicalPlan createApp(LogicalPlanConfiguration logicalPlanConfiguration)
  {
    LogicalPlan logicalPlan = new LogicalPlan();
    logicalPlanConfiguration.prepareDAG(logicalPlan, streamingApp, getName());
    return logicalPlan;
  }

  public String getName()
  {
    return name;
  }

  public String getDisplayName()
  {
    return name;
  }
}
