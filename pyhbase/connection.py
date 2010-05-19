import os
import sys

import avro.ipc as ipc
import avro.protocol as protocol

# TODO(hammer): Figure the canonical place to put this file
PROTO_FILE = os.path.join(os.path.dirname(__file__), 'schema/hbase.avpr')
PROTOCOL = protocol.parse(open(PROTO_FILE).read())

def retry_wrapper(fn):
  """a decorator to add retry symantics to any method that uses hbase"""
  def f(self, *args, **kwargs):
    try:
      return fn(self, *args, **kwargs)
    except:
      try:
        self.close()
      except:
        pass
      self.make_connection()
      return fn(self, *args, **kwargs)
  return f

class HBaseConnection(object):
  """
  Base class for HBase connections.  Supplies methods for a few basic
  queries and methods for cleanup of thrift results.
  """
  def __init__(self, host, port):
    self.host = host
    self.port = port
    self.client = None
    self.requestor = None
    self.make_connection()

  def make_connection(self, retry=2):
    """Establishes the underlying connection to HBase."""
    while retry:
      retry -= 1
      try:
        self.client = ipc.HTTPTransceiver(self.host, self.port)
        self.requestor = ipc.Requestor(PROTOCOL, self.client)
        return
      except:
        pass
    exceptionType, exception, tracebackInfo = sys.exc_info()
    raise exception

  #
  # Metadata
  #

  @retry_wrapper
  def show_tables(self):
    """Grab table names."""
    return self.requestor.request("getTableNames", {})

  @retry_wrapper
  def is_table_enabled(self, table):
    """Determine if a table is enabled."""
    return self.requestor.request("isTableEnabled", {"table": table})

  #
  # Administrative Operations
  #

  @retry_wrapper
  def enable_table(self, table):
    return self.requestor.request("enableTable", {"table": table})

  @retry_wrapper
  def disable_table(self, table):
    return self.requestor.request("disableTable", {"table": table})

  #
  # Get
  #

  @retry_wrapper
  def get(self, table, row):
    params = {"table": table, "get": {"row": row}}
    return self.requestor.request("get", params)

  #
  # Put
  #

  #
  # Delete
  #

  #
  # Scan
  #

