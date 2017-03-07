package org.apache.apex.malhar.python.runtime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.python.operator.PythonFilterOperator;
import org.apache.apex.malhar.python.operator.PythonFlatMapOperator;
import org.apache.apex.malhar.python.operator.PythonGenericOperator;
import org.apache.apex.malhar.python.operator.PythonMapOperator;
import org.apache.apex.malhar.stream.api.Option;
import org.apache.apex.malhar.stream.api.PythonApexStream;
import org.apache.apex.malhar.stream.api.impl.ApexStreamImpl;
import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.api.Operator;

/**
 * Created by vikram on 7/6/17.
 */

@InterfaceStability.Evolving
public class PythonApexStreamImpl<T> extends ApexStreamImpl<T> implements PythonApexStream<T>
{
  private static final Logger LOG = LoggerFactory.getLogger(PythonApexStreamImpl.class);


  public PythonApexStreamImpl()
  {
    super();
  }

  public PythonApexStreamImpl(ApexStreamImpl apexStream)
  {
      super(apexStream);
  }

  @Override
  public ApexStreamImpl<T> map(byte[] serializedFunction, Option... opts)
  {
    LOG.debug("Adding Python map operator");
    PythonGenericOperator<T> operator = new PythonMapOperator<T>(serializedFunction);
    return addOperator(operator, (Operator.InputPort<T>)operator.in, (Operator.OutputPort<T>)operator.out, opts);

  }

  @Override
  public ApexStreamImpl<T> flatMap(byte[] serializedFunction, Option... opts)
  {
    LOG.debug("Adding Python flatmap operator");
    PythonGenericOperator<T> operator = new PythonFlatMapOperator<T>(serializedFunction);
    return addOperator(operator, (Operator.InputPort<T>)operator.in, (Operator.OutputPort<T>)operator.out, opts);

  }

  @Override
  public ApexStreamImpl<T> filter(byte[] serializedFunction, Option... opts)
  {
    LOG.debug("Adding Python filter operator");
    PythonFilterOperator<T> operator = new PythonFilterOperator<>(serializedFunction);
    return addOperator(operator, (Operator.InputPort<T>)operator.in, (Operator.OutputPort<T>)operator.out, opts);

  }
}
