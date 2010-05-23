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

  # TODO(hammer): classify these methods like the HBase shell

  #
  # Metadata
  #

  @retry_wrapper
  def list_tables(self):
    """Grab table information."""
    return self.requestor.request("listTables", {})

  @retry_wrapper
  def describe_table(self, table):
    """Grab table information."""
    return self.requestor.request("describeTable", {"table": table})

  @retry_wrapper
  def describe_family(self, table, family):
    """Grab family information."""
    return self.requestor.request("describeFamily", {"table": table, "family": family})

  @retry_wrapper
  def is_table_enabled(self, table):
    """Determine if a table is enabled."""
    return self.requestor.request("isTableEnabled", {"table": table})

  #
  # Administrative Operations
  #

  @retry_wrapper
  def create_table(self, table, *families):
    table_descriptor = {"name": table}
    families = [{"name": family} for family in families]
    if families: table_descriptor["families"] = families
    return self.requestor.request("createTable", {"table": table_descriptor})

  @retry_wrapper
  def enable_table(self, table):
    return self.requestor.request("enableTable", {"table": table})

  @retry_wrapper
  def disable_table(self, table):
    return self.requestor.request("disableTable", {"table": table})

  #
  # Get
  #

  # TODO(hammer): Figure out how to get binary keys
  # TODO(hammer): Do this parsing logic in pyhbase-cli?
  @retry_wrapper
  def get(self, table, row, *columns):
    get = {"row": row}
    columns = [len(column) > 1 and {"family": column[0], "qualifier": column[1]} or {"family": column[0]}
               for column in map(lambda s: s.split(":"), columns)]
    if columns: get["columns"] = columns
    params = {"table": table, "get": get}
    return self.requestor.request("get", params)

  #
  # Put
  #

  # TODO(hammer): Figure out how to incorporate timestamps
  # TODO(hammer): Do this parsing logic in pyhbase-cli?
  @retry_wrapper
  def put(self, table, row, *column_values):
    put = {"row": row}
    column_values = [{"family": column.split(":")[0], "qualifier": column.split(":")[1], "value": value}
                     for column, value in zip(column_values[::2], column_values[1::2])]
    put["columnValues"] = column_values
    params = {"table": table, "put": put}
    return self.requestor.request("put", params)

  #
  # Delete
  #

  #
  # Scan
  #

  # TODO(hammer): Figure out cleaner, more functional command-line
  @retry_wrapper
  def scan(self, table, number_of_rows):
    params = {"table": table, "scan": {}}
    scanner_id = self.requestor.request("scannerOpen", params)
    results = self.requestor.request("scannerGetRows", {"scannerId": scanner_id, "numberOfRows": int(number_of_rows)})
    self.requestor.request("scannerClose", {"scannerId": scanner_id})
    return results

