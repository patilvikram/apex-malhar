package org.apache.apex.malhar.stream.api.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.python.operator.PythonGenericOperator;

/**
 * Created by vikram on 31/3/17.
 */
public class LoggerUtils
{

  public static class InputStreamConsumer extends Thread
  {
    private static final Logger LOG = LoggerFactory.getLogger(PythonGenericOperator.class);
    private InputStream is;
    private String name;
    private StreamType streamType;
    private String processId;

    public enum StreamType
    {
      ERROR, OUTPUT
    }

    public InputStreamConsumer(String name, InputStream is, StreamType streamType)
    {
      this.is = is;
      this.name = name;
      this.streamType = streamType;
//      this.processId = processId;
    }

    @Override
    public void run()
    {
      LOG.info("Starting Stream Gobbler " + this.name);
      try {

        InputStreamReader isr = new InputStreamReader(this.is);
        BufferedReader br = new BufferedReader(isr);
        String line;
        while ((line = br.readLine()) != null) {
          if (this.streamType == StreamType.ERROR) {
            LOG.error(" From other process :" + line);
          } else {
            LOG.info(" From other process :" + line);

          }
        }
      } catch (IOException exp) {
        exp.printStackTrace();
      }

      LOG.info("Exiting Stream Gobbler " + this.name);
    }
  }

  public static void captureProcessStreams(Process process)
  {
    InputStreamConsumer stdoutConsumer = new InputStreamConsumer("outputStream", process.getInputStream(), InputStreamConsumer.StreamType.OUTPUT);
    InputStreamConsumer erroConsumer = new InputStreamConsumer("errorStream", process.getErrorStream(), InputStreamConsumer.StreamType.ERROR);
    erroConsumer.start();
    stdoutConsumer.start();
  }
}
