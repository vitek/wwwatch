#!/usr/bin/env python

from distutils.core import setup

setup(name='wwwatch',
      version='0.1',
      description='Accesslog monitoring tool',
      author='Victor Makarov',
      author_email='vitja.makarov@gmail.com',
      packages=['wwwatch'],
      scripts=['bin/wwwatch'],
      url='https://github.com/vitek/wwwatch',
      license='MIT')
