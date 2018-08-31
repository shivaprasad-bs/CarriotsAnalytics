"""A setuptools based setup module.
See:
https://packaging.python.org/en/latest/distributing.html
"""

# Always prefer setuptools over distutils
from setuptools import setup,find_packages

setup(
    name='caDB',

    # Versions should comply with PEP440.  For a discussion on single-sourcing
    # the version across setup.py and the project code, see
    # https://packaging.python.org/en/latest/single_source_version.html
    version='1.0.1',

    description='Library to connect to Carriots Analytics application',
    long_description='Connect to carriots analytics application and perform load/update operations on the datasources using datasource token',

    # The project's main homepage.
    url='https://github.com/altairengineering/CarriotsAnalytics',

    # Author details
    author='Aravind S',
    author_email='aravndr@asiapac.altair.com',
    
    packages=find_packages(),
    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',

        # Indicate who your project is intended for
        'Intended Audience :: Carriots Analytics Users',
        'Topic :: Software Development :: Build Tools',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],

    # What does your project relate to?
    keywords='carriots integration',

    # Alternatively, if you want to distribute just a my_module.py, uncomment
    # this:
    #   py_modules=["my_module"],

    # List run-time dependencies here.  These will be installed by pip when
    # your project is installed. For an analysis of "install_requires" vs pip's
    # requirements files see:
    # https://packaging.python.org/en/latest/requirements.html
    install_requires=['requests','jaydebeapi','pandas','numpy'],

    # Although 'package_data' is the preferred approach, in some case you may
    # need to place data files outside of your packages. See:
    # http://docs.python.org/3.4/distutils/setupscript.html#installing-additional-files # noqa
    # In this case, 'data_file' will be installed into '<sys.prefix>/my_data'
    package_data={
            'caDB' : ['extdata/*.jar'],
    },

)
