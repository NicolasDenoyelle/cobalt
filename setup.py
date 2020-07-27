###############################################################################
# Copyright 2020 UChicago Argonne, LLC.
# (c.f. AUTHORS, LICENSE)
# For more info, see https://xgitlab.cels.anl.gov/argo/cobalt-python-wrapper
# SPDX-License-Identifier: BSD-3-Clause
##############################################################################

from setuptools import setup

setup(name='cobalt',
      version='0.1',
      description='Python wrapper for cobalt scheduler',
      url='https://xgitlab.cels.anl.gov/argo/cobalt-python-wrapper',
      author='Nicolas Denoyelle',
      author_email='ndenoyelle@anl.gov',
      license='BSD-3-Clause',
      packages=['cobalt'],
      python_requires='>=2.7',
      zip_safe=False)

