from setuptools import setup, find_packages
import sys
import os

version = '0.1'


def readme():
    dirname = os.path.dirname(os.path.abspath(__file__))
    filename = os.path.join(dirname, "README.txt")
    return open(filename).read()

setup(name='rdflibSesame2',
      version=version,
      description="A rdflib wrapper for the sesame http interface",
      long_description=readme(),
      # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      classifiers=[],
      keywords='',
      author='Robert Engsterhold',
      author_email='engsterhold@me.com',
      url='',
      license='BSD',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests', 'bench']),
      include_package_data=True,
      zip_safe=False,
      tests_require=["nose"],
      requires=[
          'requests',
          'rdflib'
      ],

      entry_points="""
          [rdf.plugins.store]
          Sesame = rdflib_sesame:wrapper.SesameStore
      """,
      )
