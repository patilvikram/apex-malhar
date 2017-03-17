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
package org.apache.apex.python;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.stream.api.ApexStream;
import org.apache.apex.malhar.stream.api.Option;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;

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

    // add SERIALIZED CODE TO LIB JARS
    // add Worker.py
    LOG.error("Populating DAG in python app");
    this.apexStream.populateDag(dag);
    String existingJars = dag.getValue(dag.LIBRARY_JARS);
    existingJars = StringUtils.isNotEmpty(existingJars) ? existingJars + "," : "";
    existingJars= existingJars + "/home/vikram/.m2/repository/net/sf/py4j/py4j/0.8.1/py4j-0.8.1.jar";
//    existingJars= existingJars +","+ "/home/vikram/Documents/py4j-0.10.4.tar.gz";
    dag.setAttribute(dag.LIBRARY_JARS, existingJars );
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


  public String launch() throws Exception
  {
    LOG.error("Launching app in python app");
    String APEX_DIRECTORY_PATH=System.getenv("APEX_HOME");
    StramAppLauncher appLauncher = null;
      Configuration configuration = this.getConf();
      configuration.set(StramAppLauncher.FILES_CONF_KEY_NAME,APEX_DIRECTORY_PATH+"/runtime/py4j-0.10.4.tar.gz");

      appLauncher = new StramAppLauncher(getName(), getConf());
      appLauncher.loadDependencies();

      PythonAppFactory appFactory = new PythonAppFactory(getName(), this);

      this.appId = appLauncher.launchApp(appFactory);
      return appId.toString();


  }

  public ApexStream getApexStream()
  {
    return apexStream;
  }

  public void setApexStream(ApexStream apexStream)
  {
    this.apexStream = apexStream;

  }

  public PythonApp fromFolder(String directoryPath ){
    apexStream = StreamFactory.fromFolder(directoryPath);
    return this;
  }


  public PythonApp fromKafka08(String zookeepers,String topic){
    apexStream = StreamFactory.fromKafka08(zookeepers,topic);
    return this;
  }

  public PythonApp fromKafka09(String zookeepers,String topic){
    apexStream = StreamFactory.fromKafka09(zookeepers,topic);
    return this;
  }

  public PythonApp setMap(String name, byte[] searializedFunction ){
    apexStream = apexStream.map_func( searializedFunction, Option.Options.name(name));
    return this;
  }

  public PythonApp setFlatMap(String name,byte[] searializedFunction ){
    apexStream = apexStream.map_func( searializedFunction, Option.Options.name(name));
    return this;
  }

  public PythonApp toConsole(String name){
    apexStream = apexStream.print(Option.Options.name(name));
    return this;
  }

  public PythonApp setConfig(String key,String value){
    getConf().set(key,value);
    return this;
  }

  public void kill() throws IOException, YarnException
  {
    YarnClient yarnClient = YarnClient.createYarnClient();
    yarnClient.init(this.conf);
    yarnClient.start();
    yarnClient.killApplication(appId);
    yarnClient.stop();
  }
}
