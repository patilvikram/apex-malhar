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

import java.io.IOException;

import org.apache.apex.malhar.stream.api.ApexStream;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.stram.client.StramAppLauncher;

public class StreamingApp
{
  private ApexStream apexStream;
  private String name = "testName";
  private ApplicationId appId;
  private Configuration conf;

  public StreamingApp(String name, Configuration conf)
  {
    this.name = name;
    this.conf = conf;
  }

  public StreamingApp fromFolder(String folderName)
  {
    apexStream = StreamFactory.fromFolder(folderName);
    return this;
  }

  public StreamingApp printStream()
  {
    apexStream.print();
    return this;
  }

  public String launchDAG() throws Exception
  {
    ApexPythonApplication app = new ApexPythonApplication(apexStream);

    StramAppLauncher appLauncher = new StramAppLauncher(name, conf);
    appLauncher.loadDependencies();
    StreamingAppFactory appFactory = new StreamingAppFactory(app, name);
    this.appId = appLauncher.launchApp(appFactory);

    return appId.toString();
  }

  public void kill() throws IOException, YarnException
  {
    YarnClient yarnClient = YarnClient.createYarnClient();
    yarnClient.init(this.conf);
    yarnClient.start();
    yarnClient.killApplication(appId);
    yarnClient.stop();
  }

  public static class ApexPythonApplication implements StreamingApplication
  {
    private final ApexStream apexStream;

    public ApexPythonApplication(ApexStream stream)
    {
      this.apexStream = stream;
    }

    @Override
    public void populateDAG(DAG dag, Configuration configuration)
    {
      apexStream.populateDag(dag);
    }
  }
}