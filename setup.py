from setuptools import setup, find_packages

setup(name="PyHBase",
      version='0.0.2',
      description="High-level Python interface to HBase",
      url="http://github.com/hammer/pyhbase/tree/master",
      packages=find_packages(),
      include_package_data=True,
      author="Jeff Hammerbacher",
      author_email="hammer@cloudera.com",
      keywords="database hbase",
      scripts=['examples/pyhbase-cli'],
      install_requires=['Thrift', 'python-hbase'])
