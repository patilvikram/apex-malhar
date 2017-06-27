package org.apache.apex.malhar.python.operator.interfaces;

/**
 * Created by vikram on 27/6/17.
 */
public interface Reduce<T>
{
  public abstract T reduce(T input1, T input2);
}
