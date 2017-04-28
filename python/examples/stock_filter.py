

def f(a):
  input_data=a.split(',')
  if float(input_data[2])> 100:
     return True
  return False

from pyapex import createApp
a=createApp('python_app').fromKafka08('localhost:2181','test_topic') \
  .setFilter('filter_operator',f) \
  .toConsole(name='endConsole') \
  .launch(False)

