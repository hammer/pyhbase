#! /usr/bin/env python
import sys

from pyhbase.connection import HBaseConnection

# TODO(hammer): Use optparse or python-gflags here
if __name__=="__main__":
  def usage():
    print """
  Usage: %s [-h host[:port]] command [arg1 [arg2...]]

  Commands:
      show_tables
      describe_table table_name
      get_table_regions table_name
      is_table_enabled table_name

      enable_table table_name
      disable_table table_name
      compact table_name
      major_compact table_name

      get table_name row_id
      get_cell_versions table_name row_id column number_of_versions

      put table_name row_id column_family column value

      delete_row table_name row_id
      delete_cells table_name row_id column

      scan table_name start_row_id column_or_column_family number_of_rows
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

  if cmd == 'show_tables':
    if len(args) != 0:
      usage()
      sys.exit(1)
    print connection.show_tables(*args)
  elif cmd == 'describe_table':
    if len(args) != 1:
      usage()
      sys.exit(1)
    print connection.describe_table(*args)
  elif cmd == 'get_table_regions':
    if len(args) != 1:
      usage()
      sys.exit(1)
    print connection.get_table_regions(*args)
  elif cmd == 'is_table_enabled':
    if len(args) != 1:
      usage()
      sys.exit(1)
    print connection.is_table_enabled(*args)
  elif cmd == 'enable_table':
    if len(args) != 1:
      usage()
      sys.exit(1)
    print connection.enable_table(*args)
  elif cmd == 'disable_table':
    if len(args) != 1:
      usage()
      sys.exit(1)
    print connection.disable_table(*args)
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
  elif cmd == 'get':
    if len(args) != 2:
      usage()
      sys.exit(1)
    print connection.get(*args)
  elif cmd == 'get_cell_versions':
    if len(args) != 4:
      usage()
      sys.exit(1)
    print connection.get_cell_versions(*args)
  elif cmd == 'put':
    if len(args) != 5:
      usage()
      sys.exit(1)
    print connection.put(*args)
  elif cmd == 'delete_row':
    if len(args) != 2:
      usage()
      sys.exit(1)
    print connection.delete_row(*args)
  elif cmd == 'delete_cells':
    if len(args) != 3:
      usage()
      sys.exit(1)
    print connection.delete_cellsn(*args)
  elif cmd == 'scan':
    if len(args) != 4:
      usage()
      sys.exit(1)
    print connection.scan(*args)
  else:
    usage()
    sys.exit(1)

  connection.close()

