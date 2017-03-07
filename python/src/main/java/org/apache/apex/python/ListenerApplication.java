/**
 * Created by vikram on 1/3/17.
 */
package org.apache.apex.python;

import java.util.ArrayList;
import java.util.List;

import py4j.GatewayServer;

public class ListenerApplication
{

  List<ExampleListener> listeners = new ArrayList<ExampleListener>();

  public void registerListener(ExampleListener listener)
  {
    listeners.add(listener);
  }

  public void notifyAllListeners()
  {
    for (ExampleListener listener : listeners) {
      Object returnValue = listener.notify(this);
      System.out.println(returnValue);
    }
  }

  @Override
  public String toString()
  {
    return "<ListenerApplication> instance";
  }

  public static void main(String[] args)
  {
    ListenerApplication application = new ListenerApplication();
    GatewayServer server = new GatewayServer(application);
    server.start(true);

    Thread newThread = new Thread(new TestThread(application));
    newThread.start();
  }
}