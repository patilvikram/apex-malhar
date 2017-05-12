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
package org.apache.apex.malhar.python;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator;

import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.stream.api.ApexStream;
import org.apache.apex.malhar.stream.api.Option;
import org.apache.apex.malhar.stream.api.impl.ApexWindowedStreamImpl;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.contrib.kafka.KafkaSinglePortOutputOperator;
import com.datatorrent.stram.client.StramAppLauncher;

public class PythonApp implements StreamingApplication
{

  private ApexStream apexStream = null;
  private StreamFactory streamFactory;
  private ApplicationId appId = null;
  private static final Logger LOG = LoggerFactory.getLogger(PythonApp.class);
  private String py4jSrcZip = "py4j-0.10.4-src.zip";

  private PythonAppManager manager = null;

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

  public PythonApp(String name, ApplicationId appId)
  {
    this.appId = appId;
    this.name = name;
  }

  private Configuration conf;

  public PythonApp()
  {
    this.conf = new Configuration(true);
    this.conf.set("dt.loggers.level", "com.datatorrent.*:INFO,org.apache.*:DEBUG,httpclient.wire.*:INFO;org.apache.apex.malhar.python.*:DEBUG");
  }

  public void populateDAG(DAG dag, Configuration conf)
  {

    LOG.trace("Populating DAG in python app");
    this.apexStream.populateDag(dag);

  }

  public void setRequiredJARFiles()
  {
    String APEX_DIRECTORY_PATH = System.getenv("PYAPEX_HOME");
    LOG.debug("PYAPEX_HOME:" + APEX_DIRECTORY_PATH);
    File dir = new File(APEX_DIRECTORY_PATH + "/jars/");
    File[] files = dir.listFiles();
    ArrayList<String> jarFiles = new ArrayList<String>();
    for (File jarFile : files) {
      LOG.info("FOUND FILES " + jarFile.getAbsolutePath());
      jarFiles.add(jarFile.getAbsolutePath());

    }
    jarFiles.add(APEX_DIRECTORY_PATH + "/runtime/" + py4jSrcZip);
    jarFiles.add(APEX_DIRECTORY_PATH + "/runtime/worker.py");
    extendExistingConfig(StramAppLauncher.LIBJARS_CONF_KEY_NAME, jarFiles);
//    this.getClassPaths();
  }

  public List<String> getClassPaths()
  {
    LOG.info("PROCESSING CLASSPATH");
    List<String> paths = new ArrayList<>();
    ClassLoader cl = ClassLoader.getSystemClassLoader();
    URL[] urls = ((URLClassLoader)cl).getURLs();
    for (URL url : urls) {
      LOG.info("FOUND FILE PATH" + url.getFile());
      paths.add(url.getFile());
    }
    return paths;
  }

  public void setRequiredRuntimeFiles()
  {
    String APEX_DIRECTORY_PATH = System.getenv("PYAPEX_HOME");
    ArrayList<String> files = new ArrayList<String>();
    files.add(APEX_DIRECTORY_PATH + "/runtime/" + py4jSrcZip);
    files.add(APEX_DIRECTORY_PATH + "/runtime/worker.py");
    extendExistingConfig(StramAppLauncher.FILES_CONF_KEY_NAME, files);

  }

  public void extendExistingConfig(String fileVariable, ArrayList<String> fileList)
  {
    Configuration configuration = this.getConf();
    String fileCSV = configuration.get(fileVariable);
    String filesCSVToAppend = StringUtils.join(fileList, ",");

    if (StringUtils.isEmpty(fileCSV)) {
      fileCSV = filesCSVToAppend;

    } else {
      fileCSV = fileCSV + "," + filesCSVToAppend;
    }

    configuration.set(fileVariable, fileCSV);
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

  public String launch(boolean local) throws Exception
  {
    PythonAppManager.LaunchMode mode = PythonAppManager.LaunchMode.HADOOP;
    if (local) {
      mode = PythonAppManager.LaunchMode.LOCAL;
    }

    this.setRequiredJARFiles();
    this.setRequiredRuntimeFiles();
    this.manager = new PythonAppManager(this, mode);

    return manager.launch();
  }

  public void runLocal()
  {
    this.apexStream.runEmbedded(true, 0, null);
  }

  public ApexStream getApexStream()
  {
    return apexStream;
  }

  public void setApexStream(ApexStream apexStream)
  {
    this.apexStream = apexStream;

  }

  public PythonApp fromFolder(String directoryPath)
  {
    apexStream = StreamFactory.fromFolder(directoryPath);
    return this;
  }

  public PythonApp fromKafka08(String zookeepers, String topic)
  {
    apexStream = StreamFactory.fromKafka08(zookeepers, topic);
    return this;
  }

  public PythonApp fromData(List<Object> inputs)
  {
    apexStream = StreamFactory.fromData(inputs);
    return this;
  }

  public PythonApp fromKafka09(String zookeepers, String topic)
  {
    apexStream = StreamFactory.fromKafka09(zookeepers, topic);
    return this;
  }

  public PythonApp setMap(String name, byte[] searializedFunction)
  {
    apexStream = apexStream.map_func(searializedFunction, Option.Options.name(name));
    return this;
  }

  public PythonApp setFlatMap(String name, byte[] searializedFunction)
  {
    apexStream = apexStream.flatmap_func(searializedFunction, Option.Options.name(name));
    return this;
  }

  public PythonApp setFilter(String name, byte[] searializedFunction)
  {
    apexStream = apexStream.filter_func(searializedFunction, Option.Options.name(name));
    return this;
  }

  public PythonApp window()
  {
    apexStream = apexStream.window(new WindowOption.GlobalWindow());
    return this;
  }

  public PythonApp count(String name)
  {
    if (apexStream instanceof ApexWindowedStreamImpl) {
      apexStream = ((ApexWindowedStreamImpl)apexStream).count(Option.Options.name(name));
      return this;
    }
    return this;
  }

  public PythonApp toConsole(String name)
  {
    apexStream = apexStream.print(Option.Options.name(name));
    return this;
  }

  public PythonApp toKafka08(String name, String topic, Map<String, String> properties)
  {
    KafkaSinglePortOutputOperator kafkaOutputOperator = new KafkaSinglePortOutputOperator();
    kafkaOutputOperator.setTopic(topic);
    List<String> propertyList = new ArrayList<String>();
    for (String key : properties.keySet()) {
      propertyList.add(key + "=" + properties.get(key));
    }

    String producerConfigs = StringUtils.join(propertyList, ",");
    LOG.debug("PropertyList for kafka producer" + producerConfigs);
    kafkaOutputOperator.setProducerProperties(producerConfigs);
    apexStream = apexStream.endWith(kafkaOutputOperator, kafkaOutputOperator.inputPort, Option.Options.name(name));
    return this;
  }

  public PythonApp toFolder(String name, String fileName, String directoryName)
  {

    GenericFileOutputOperator<String> outputOperator = new GenericFileOutputOperator<>();
    outputOperator.setFilePath(directoryName);
    outputOperator.setOutputFileName(fileName);
    outputOperator.setConverter(new GenericFileOutputOperator.StringToBytesConverter());
    apexStream = apexStream.endWith(outputOperator, outputOperator.input, Option.Options.name(name));
    return this;
  }

  public PythonApp setConfig(String key, String value)
  {
    getConf().set(key, value);
    return this;
  }

  public void kill() throws Exception
  {
    if (manager == null) {
      throw new Exception("Application is not running yet");

    }
    manager.shutdown();
  }
}
