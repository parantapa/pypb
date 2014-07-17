from distutils.core import setup

setup(
    name             = 'PyPB',
    version          = '0.2',
    author           = 'Parantapa Bhattacharya',
    author_email     = 'pb@parantapa.net',
    packages         = ['pypb'],
    url              = 'github.com/parantapa/pypb',
    description      = 'Python modules for PB',
    long_description = open('README.md').read(),
    install_requires = [
        "logbook",
        "python-daemon",
        "pyzmq",
        "requests",
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 2.7",
        "Topic :: Utilities"
    ],
)
