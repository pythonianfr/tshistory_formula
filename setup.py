from pathlib import Path
from setuptools import setup

from tshistory_formula import __version__


doc = Path(__file__).parent / 'README.md'


setup(name='tshistory_formula',
      version=__version__,
      author='Pythonian',
      author_email='aurelien.campeas@pythonian.fr',
      url='https://hg.sr.ht/~pythonian/tshistory_formula',
      description='Computed timeseries plugin for `tshistory`',
      long_description=doc.read_text(),
      long_description_content_type='text/markdown',

      packages=['tshistory_formula'],
      zip_safe=False,
      install_requires=[
          'tshistory >= 0.22.1',
          'holidays == 0.75, < 1',
          'pycountry >= 24.6.1',
          'psyl >= 0.8',
          'python-icron',
          'pytest-golden',
      ],
      package_data={'tshistory_formula': [
          'schema.sql'
      ]},
      entry_points={
          'tshistory.migrate.Migrator': [
              'migrator=tshistory_formula.migrate:Migrator'
          ],
          'tshclass': [
              'tshclass=tshistory_formula.tsio:timeseries'
          ],
          'httpclient': [
              'httpclient=tshistory_formula.http:formula_httpclient'
          ],
          'forceimports': [
              'forceimports=tshistory_formula.search:IMPORTCALLBACK'
          ]
      },
      classifiers=[
          'Development Status :: 4 - Beta',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)',
          'Operating System :: OS Independent',
          'Programming Language :: Python :: 3',
          'Topic :: Database',
          'Topic :: Scientific/Engineering',
          'Topic :: Software Development :: Version Control'
      ]
)
