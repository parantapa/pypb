from distutils.core import setup

setup(
    name             = 'PyPB',
    version          = '0.1',
    author           = 'Parantapa Bhattacharya',
    author_email     = 'pb@parantapa.net',
    packages         = ['pypb'],
    url              = 'github.com/parantapa/pypb',
    license          = 'LICENSE.txt',
    description      = 'Python modules for PB',
    long_description = open('README.md').read(),
    install_requires = [
        "logbook",
        "python-daemon",
        "pyzmq",
        "requests",
    ],
)
