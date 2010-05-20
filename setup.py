from setuptools import setup, find_packages

setup(name="PyHBase",
      version='0.0.5',
      description="High-level Python interface to HBase",
      url="http://github.com/hammer/pyhbase/",
      packages=['pyhbase'],
      package_dir={'pyhbase': 'pyhbase'},
      package_data={'pyhbase': ['schema/*.avpr']},
      author="Jeff Hammerbacher",
      author_email="hammer@cloudera.com",
      keywords="database hbase avro",
      scripts=['examples/pyhbase-cli'],
      install_requires=['avro'])
