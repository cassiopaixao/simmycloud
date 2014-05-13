from distutils.core import setup, Extension
import glob
 
module = Extension('rbf', language = 'c', sources = ['rbf.c'] + glob.glob('routines/*c'))
 
setup (name = 'rbf',
        version = '1.0',
        description = 'Utility package to model data using a radial basis function (rbf) ansatz.',
        maintainer = 'Daniel Cordeiro',
        maintainer_email = 'danielc@ime.usp.br',
        ext_modules = [module])
