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
          'tshistory >= 0.18',
          'holidays >= 0.21',
          'pycountry >= 22.3.5',
          'psyl >= 0.8'
      ],
      package_data={'tshistory_formula': [
          'schema.sql'
      ]},
      entry_points={
          'tshistory.subcommands': [
              'typecheck-formula=tshistory_formula.cli:typecheck_formula',
              'test-formula=tshistory_formula.cli:test_formula',
              'formula-init-db=tshistory_formula.cli:init_db',
              'migrate-to-formula-groups=tshistory_formula.cli:migrate_to_groups',
              'migrate-to-content-cache=tshistory_formula.cli:migrate_to_content_hash',
              'migrate-to-formula-patch=tshistory_formula.cli:migrate_to_formula_patch',
              'migrate-formula-schema=tshistory_formula.cli:migrate_formula_schema',
              'migrate-trig-formulas=tshistory_formula.cli:migrate_trig_formulas',
              'migrate-sub-formulas=tshistory_formula.cli:migrate_sub_formulas',
              'rename-operators=tshistory_formula.cli:rename_operators',
              'migrate-to-dependants=tshistory_formula.cli:migrate_to_dependants',
              'fix-formula-groups-metadata=tshistory_formula.cli:fix_formula_groups_metadata',
              'shell=tshistory_formula.cli:shell'
          ],
          'tshistory.migrate.run_migrations': [
              'run_migrations=tshistory_formula.migrate:run_migrations'
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
