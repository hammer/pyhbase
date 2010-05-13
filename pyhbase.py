#! /usr/bin/env python
import sys

from thrift import Thrift
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from hbase import ttypes
from hbase.Hbase import Client, ColumnDescriptor, Mutation

def retry_wrapper(fn):
  """a decorator to add retry symantics to any method that uses hbase"""
  def f(self, *args, **kwargs):
    try:
      return fn(self, *args, **kwargs)
    except self.hbaseThriftExceptions:
      try:
        self.close()
      except self.hbaseThriftExceptions:
        pass
      self.make_connection()
      return fn(self, *args, **kwargs)
  return f

class HBaseConnection(object):
  """
  Base class for HBase connections.  Supplies methods for a few basic
  queries and methods for cleanup of thrift results.
  """
  def __init__(self, host, port,
               thrift=Thrift,
               tsocket=TSocket,
               ttrans=TTransport,
               protocol=TBinaryProtocol,
               ttp=ttypes,
               client=Client,
               column=ColumnDescriptor,
               mutation=Mutation,
               logger=None):
    self.host = host
    self.port = port
    self.thriftModule = thrift
    self.tsocketModule = tsocket
    self.transportModule = ttrans
    self.protocolModule = protocol
    self.ttypesModule = ttp
    self.clientClass = client
    self.columnClass = column
    self.mutationClass = mutation
    self.logger = logger
    self.hbaseThriftExceptions = (self.ttypesModule.IOError,
                                  self.ttypesModule.IllegalArgument,
                                  self.ttypesModule.AlreadyExists,
                                  self.thriftModule.TException)
    self.make_connection()

  def make_connection(self, retry=2):
    """Establishes the underlying connection to hbase"""
    while retry:
      retry -= 1
      try:
        # Make socket
        transport = self.tsocketModule.TSocket(self.host, self.port)
        # Buffering is critical. Raw sockets are very slow
        self.transport = self.transportModule.TBufferedTransport(transport)
        # Wrap in a protocol
        self.protocol = self.protocolModule.TBinaryProtocol(self.transport)
        # Create a client to use the protocol encoder
        self.client = self.clientClass(self.protocol)
        # Connect!
        self.transport.open()
        return
      except self.hbaseThriftExceptions, x:
        pass
    exceptionType, exception, tracebackInfo = sys.exc_info()
    raise exception

  def close(self):
    """
    Close the hbase connection
    """
    self.transport.close()

  def _make_rows_nice(self,client_result_object):
    """
    Apply _make_row_nice to multiple rows
    """
    res = [self._make_row_nice(row) for row in client_result_object]
    return res

  def _make_row_nice(self,client_row_object):
    """
    Pull out the contents of the thrift column result objects into a python dict
    """
    return dict(((x,y.value) for x,y in client_row_object.columns.items()))

  @retry_wrapper
  def describe_table(self,table_name):
    return self.client.getColumnDescriptors(table_name)

  @retry_wrapper
  def get_full_row(self,table_name, row_id):
    """
    Get back every column value for a specific row_id
    """
    return self._make_rows_nice(self.client.getRow(table_name, row_id))

  @retry_wrapper
  def compact(self, table_name):
    return self.client.compact(table_name)

  @retry_wrapper
  def major_compact(self, table_name):
    return self.client.majorCompact(table_name)

  @retry_wrapper
  def get_table_regions(self, table_name):
    return self.client.getTableRegions(table_name)

  @retry_wrapper
  def put(self, table_name, r, cf, c, v):
    m = self.mutationClass(0, ':'.join([cf,c]), v)
    return self.client.mutateRow(table_name, r, [m])

  @retry_wrapper
  def scan(self, table_name, r, cf, n):
    s = self.client.scannerOpen(table_name, r, [cf])
    return self.client.scannerGetList(s, int(n))

if __name__=="__main__":
  def usage():
    print """
  Usage: %s [-h host[:port]] command [arg1 [arg2...]]

  Commands:
      compact table_name
      major_compact table_name

      describe_table table_name
      get_table_regions table_name

      get_full_row table_name row_id
      put table_name row_id column_family column value
      scan table_name start_row_id columns number_of_rows
  """ % sys.argv[0]

  if len(sys.argv) <= 1 or sys.argv[1] == '--help':
    usage()
    sys.exit(0)

  host = 'localhost'
  port = 9090
  argi = 1

  if sys.argv[argi] == '-h':
    parts = sys.argv[argi+1].split(':')
    host = parts[0]
    if len(parts) == 2:
      port = int(parts[1])
    argi += 2

  cmd = sys.argv[argi]
  args = sys.argv[argi+1:]

  connection = HBaseConnection(host, port)

  if cmd == 'describe_table':
    if len(args) != 1:
      usage()
      sys.exit(1)
    print connection.describe_table(*args)
  elif cmd == 'get_full_row':
    if len(args) != 2:
      usage()
      sys.exit(1)
    print connection.get_full_row(*args)
  elif cmd == 'compact':
    if len(args) != 1:
      usage()
      sys.exit(1)
    print connection.compact(*args)
  elif cmd == 'major_compact':
    if len(args) != 1:
      usage()
      sys.exit(1)
    print connection.major_compact(*args)
  elif cmd == 'get_table_regions':
    if len(args) != 1:
      usage()
      sys.exit(1)
    print connection.get_table_regions(*args)
  elif cmd == 'put':
    if len(args) != 5:
      usage()
      sys.exit(1)
    print connection.put(*args)
  elif cmd == 'get_table_regions':
    if len(args) != 1:
      usage()
      sys.exit(1)
    print connection.get_table_regions(*args)
  elif cmd == 'scan':
    if len(args) != 4:
      usage()
      sys.exit(1)
    print connection.scan(*args)
  else:
    usage()
    sys.exit(1)

  connection.close()

