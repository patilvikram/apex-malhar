package org.apache.apex.malhar.python.operator.interfaces;

import org.apache.apex.malhar.lib.window.accumulation.Reduce;

public interface PythonReduceWorker<T> extends PythonAccumulatorWorker<T>, Reduce<T>
{
}
