package org.apache.apex.malhar.python;

import java.io.IOException;

import javax.ws.rs.core.MediaType;

import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;

import com.sun.jersey.api.client.WebResource;

import com.datatorrent.api.LocalMode;
import com.datatorrent.stram.cli.ApexCli;
import com.datatorrent.stram.client.StramAgent;
import com.datatorrent.stram.client.StramAppLauncher;
import com.datatorrent.stram.util.WebServicesClient;
import com.datatorrent.stram.webapp.StramWebServices;

public class PythonAppManager
{
  private LaunchMode mode;
  private Object appIdenfier;
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

    try {
      if (mode == LaunchMode.LOCAL) {
        app.runLocal();
        return "LocalMode";
      } else {
        StramAppLauncher appLauncher = null;
        appLauncher = new StramAppLauncher(app.getName(), app.getConf());
        appLauncher.loadDependencies();

        PythonAppFactory appFactory = new PythonAppFactory(app.getName(), app);

        this.appIdenfier = appLauncher.launchApp(appFactory);
        return this.appIdenfier.toString();
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  public void shutdown()
  {
    if (mode == LaunchMode.LOCAL) {
      ((LocalMode.Controller)this.appIdenfier).shutdown();
    } else {
      try {
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(app.getConf());
        yarnClient.start();

        yarnClient.killApplication((ApplicationId)this.appIdenfier);
        yarnClient.stop();
      } catch (YarnException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }

    }
  }

  public void test(){

    for (ApplicationReport app : apps) {
      try {
        JSONObject response = getResource(new StramAgent.StramUriSpec().path(StramWebServices.PATH_SHUTDOWN), app, new WebServicesClient.WebServicesHandler<JSONObject>()
        {
          @Override
          public JSONObject process(WebResource.Builder webResource, Class<JSONObject> clazz)
          {
            return webResource.accept(MediaType.APPLICATION_JSON).post(clazz, new JSONObject());
          }

        });
        if (consolePresent) {
          System.out.println("Shutdown requested: " + response);
        }
        currentApp = null;
      } catch (Exception e) {
        throw new ApexCli.CliException("Failed to request shutdown for appid " + app.getApplicationId().toString(), e);
      }
    }
  }
}
