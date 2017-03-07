package org.apache.apex.python;

/**
 * Created by vikram on 1/3/17.
 */
public class TestThread implements Runnable

{
  private ListenerApplication application = null;

  public TestThread(ListenerApplication application)
  {
    this.application = application;
  }

  @Override
  public void run()
  {

    while (true) {

      try {
        Thread.sleep(1000);
        this.application.notifyAllListeners();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

    }
  }
}
