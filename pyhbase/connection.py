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
    """Establishes the underlying connection to HBase."""
    while retry:
      retry -= 1
      try:
        transport = self.tsocketModule.TSocket(self.host, self.port)
        self.transport = self.transportModule.TBufferedTransport(transport)
        self.protocol = self.protocolModule.TBinaryProtocol(self.transport)
        self.client = self.clientClass(self.protocol)
        self.transport.open()
        return
      except self.hbaseThriftExceptions, x:
        pass
    exceptionType, exception, tracebackInfo = sys.exc_info()
    raise exception

  def close(self):
    """
    Close the HBase connection.
    """
    self.transport.close()

  def _make_rows_nice(self,client_result_object):
    """
    Apply _make_row_nice to multiple rows.
    """
    res = [self._make_row_nice(row) for row in client_result_object]
    return res

  def _make_row_nice(self,client_row_object):
    """
    Pull out the contents of the Thrift TRowResult objects into a dict.
    """
    return dict(((x,(y.value,y.timestamp))
                for x, y in client_row_object.columns.items()))

  #
  # Metadata
  #

  @retry_wrapper
  def show_tables(self):
    """Grab table names."""
    return self.client.getTableNames()

  @retry_wrapper
  def describe_table(self, table_name):
    """Get information about the column families in a table."""
    return self.client.getColumnDescriptors(table_name)

  @retry_wrapper
  def get_table_regions(self, table_name):
    return self.client.getTableRegions(table_name)

  @retry_wrapper
  def is_table_enabled(self, table_name):
    """Determine if a table is enabled."""
    return self.client.isTableEnabled(table_name)

  #
  # Administrative Operations
  #

  @retry_wrapper
  def enable_table(self, table_name):
    return self.client.enableTable(table_name)

  @retry_wrapper
  def disable_table(self, table_name):
    return self.client.disableTable(table_name)

  @retry_wrapper
  def compact(self, table_name):
    return self.client.compact(table_name)

  @retry_wrapper
  def major_compact(self, table_name):
    return self.client.majorCompact(table_name)

  #
  # Get
  #

  @retry_wrapper
  def get(self, table_name, row_id):
    """
    Get back every column value for a specific row_id.
    """
    return self._make_rows_nice(self.client.getRow(table_name, row_id))

  @retry_wrapper
  def get_cell_versions(self, table_name, row_id, column, n):
    return self.client.getVer(table_name, row_id, column, int(n))

  #
  # Put
  #

  @retry_wrapper
  def put(self, table_name, r, cf, c, v):
    m = self.mutationClass(0, ':'.join([cf,c]), v)
    return self.client.mutateRow(table_name, r, [m])

  #
  # Delete
  #

  @retry_wrapper
  def delete_row(self, table_name, r):
    return self.client.deleteAllRow(table_name, r)

  @retry_wrapper
  def delete_cells(self, table_name, r, column):
    return self.client.deleteAll(table_name, r, column)

  #
  # Scan
  #

  @retry_wrapper
  def scan(self, table_name, r, column_or_cf, n):
    s = self.client.scannerOpen(table_name, r, [column_or_cf])
    result = self.client.scannerGetList(s, int(n))
    self.client.scannerClose(s)
    return result
