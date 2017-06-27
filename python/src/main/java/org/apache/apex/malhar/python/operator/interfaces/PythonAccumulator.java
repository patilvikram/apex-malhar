package org.apache.apex.malhar.python.operator.interfaces;

import org.apache.apex.malhar.lib.window.Accumulation;

public class PythonAccumulator implements Accumulation<Object,Object,Object>
  {


    private byte[] serialiedData;

    private PythonAcummlationWorkerProxy proxy;

    public PythonAccumulator(byte[] serialiedData)
    {
      this.serialiedData = serialiedData;
    }

    @Override
    public Object defaultAccumulatedValue()
    {
      return null;
    }

    @Override
    public Object accumulate(Object accumulatedValue, Object input)
    {

      if( proxy == null )
      {
        return proxy.accumulate(accumulatedValue,input);
      }

      return null;
    }

    @Override
    public Object merge(Object accumulatedValue1, Object accumulatedValue2)
    {

      if( proxy == null )
      {
        return proxy.merge(accumulatedValue1,accumulatedValue2);
      }
      return null;
    }

    @Override
    public Object getOutput(Object accumulatedValue)
    {
      return proxy.getOutput(accumulatedValue);
    }

    @Override
    public Object getRetraction(Object value)
    {
      return getProxy().getRetraction(value);
    }


    public byte[] getSerialiedData()
    {
      return serialiedData;
    }

    public void setSerialiedData(byte[] serialiedData)
    {
      this.serialiedData = serialiedData;
    }


    public PythonAcummlationWorkerProxy getProxy()
    {
      return proxy;
    }

    public void setProxy(PythonAcummlationWorkerProxy proxy)
    {
      this.proxy = proxy;
    }

  }
