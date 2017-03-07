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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.stream.api.ApexStream;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.stram.client.StramAppLauncher;

/**
 * Created by vikram on 17/2/17.
 */

public class PythonApp implements StreamingApplication
{

  private ApexStream apexStream = null;
  private StreamFactory streamFactory;
  private ApplicationId appId = null;
  private static final Logger LOG = LoggerFactory.getLogger(PythonApp.class);

  public String getName()
  {
    return name;
  }

  private String name;

  public PythonApp(String name)
  {
    this.name = name;
    this.conf = new Configuration(true);
  }

  private Configuration conf;

  public PythonApp()
  {
    this.conf = new Configuration(true);
    this.conf.set("dt.loggers.level","com.datatorrent.*:INFO,org.apache.*:DEBUG,httpclient.wire.*:INFO;");
  }

  public void populateDAG(DAG dag, Configuration conf)
  {
    LOG.error("Populating DAG in python app");
    this.apexStream.populateDag(dag);
    String existingJars = dag.getValue(dag.LIBRARY_JARS);
    existingJars = StringUtils.isNotEmpty(existingJars) ? existingJars + "," : "";
    dag.setAttribute(dag.LIBRARY_JARS, existingJars + "/home/vikram/.m2/repository/net/sf/py4j/py4j/0.8.1/py4j-0.8.1.jar");
    LOG.error("DAG VALUE LIBRARY JARS" + dag.getValue(DAG.LIBRARY_JARS));
  }

  public Configuration getConf()
  {
    return conf;
  }

  public void setConf(Configuration conf)
  {
    this.conf = conf;
  }

  public StreamFactory getStreamFactory()
  {
    if (streamFactory == null) {
      streamFactory = new StreamFactory();
    }
    return streamFactory;
  }

  public String launch()
  {
    StramAppLauncher appLauncher = null;
    try {
      appLauncher = new StramAppLauncher(getName(), getConf());
      appLauncher.loadDependencies();

      PythonAppFactory appFactory = new PythonAppFactory(getName(), this);

      this.appId = appLauncher.launchApp(appFactory);
      return appId.toString();

    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;

  }

  public ApexStream getApexStream()
  {
    return apexStream;
  }

  public void setApexStream(ApexStream apexStream)
  {
    this.apexStream = apexStream;

  }

}
