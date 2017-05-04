package org.apache.apex.malhar.python;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;

import com.datatorrent.api.LocalMode;
import com.datatorrent.stram.client.StramAppLauncher;

public class PythonAppManager
{
  private LaunchMode mode;
  private Object appIdentifier;
  private PythonApp app = null;
  private static final Logger LOG = LoggerFactory.getLogger(PythonApp.class);

  public enum LaunchMode
  {
    LOCAL, HADOOP, EMR;
  }

  public PythonAppManager(PythonApp app, LaunchMode mode)
  {
    this.app = app;
    this.mode = mode;
  }

  public String launch()
  {

    LOG.error("Launching app in python app");
    String APEX_DIRECTORY_PATH = System.getenv("APEX_HOME");

    try {
      if (mode == LaunchMode.LOCAL) {
        LocalMode lma = LocalMode.newInstance();
        lma.prepareDAG(app, app.getConf());
        LocalMode.Controller lc = lma.getController();
        lc.runAsync();
        appIdentifier = lc;
        return "LocalMode";
      } else {
        StramAppLauncher appLauncher = null;
        appLauncher = new StramAppLauncher(app.getName(), app.getConf());
        appLauncher.loadDependencies();

        PythonAppFactory appFactory = new PythonAppFactory(app.getName(), app);

        this.appIdentifier = appLauncher.launchApp(appFactory);
        return this.appIdentifier.toString();
      }

    } catch (Exception e) {
      e.printStackTrace();

      LOG.error("FAILED TO LAUNCH PYTHON STREAMING APPLICATION ");
      LOG.error("Encountered Exception " + e.getMessage());
    }
    return null;
  }

  public void shutdown()
  {
    if (mode == LaunchMode.LOCAL) {
      ((LocalMode.Controller)this.appIdentifier).shutdown();
    } else {
      try {
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(app.getConf());
        yarnClient.start();

        yarnClient.killApplication((ApplicationId)this.appIdentifier);
        yarnClient.stop();
      } catch (YarnException e) {
        e.printStackTrace();
        LOG.error("FAILED TO SHUTDOWN PYTHON STREAMING APPLICATION ");
        LOG.error("Encountered Exception " + e.getMessage());
      } catch (IOException e) {
        LOG.error("FAILED TO SHUTDOWN PYTHON STREAMING APPLICATION ");
        LOG.error("Encountered Exception " + e.getMessage());
      }

    }
  }
}
