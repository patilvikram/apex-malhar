package org.apache.apex.malhar.python.operator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.PythonConstants;
import org.apache.apex.malhar.lib.window.impl.WindowedOperatorImpl;
import org.apache.apex.malhar.python.runtime.PythonServer;
import org.apache.apex.malhar.python.runtime.PythonWorkerContext;
import org.apache.apex.malhar.python.runtime.PythonWorkerProxy;
import org.apache.apex.malhar.python.util.LoggerUtils;
import org.apache.apex.malhar.python.util.NetworkUtils;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

import py4j.GatewayConnection;
import py4j.GatewayServer;
import py4j.GatewayServerListener;
import py4j.Py4JServerConnection;

public class PythonWindowedOperator<T> extends WindowedOperatorImpl
{

  private static final Logger LOG = LoggerFactory.getLogger(PythonWindowedOperator.class);
  private PythonServer server = null;

//  protected transient GatewayServer server = null;
//  protected transient PythonGenericOperator.PythonGatewayServerListenser py4jListener = null;
//  protected transient PythonWorkerProxy<T> pythonWorkerProxy = null;
  protected byte[] serializedFunction = null;
  protected transient PythonConstants.OpType operationType = null;
//  private PythonWorkerContext pythonWorkerContext = null;
//  protected Map<java.lang.String, java.lang.String> environementData = new HashMap<java.lang.String, java.lang.String>();



  public PythonWindowedOperator(byte[] serializedFunc)
  {
    this.serializedFunction = serializedFunc;
    this.server = new PythonServer(this.operationType, serializedFunc);
  }

  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    server.setup();
  }

  public void teardown()
  {
    if (server != null) {
      server.shutdown();
    }
  }


}
