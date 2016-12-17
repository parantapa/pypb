from setuptools import setup

setup(
    name="PyPB",
    version="1.0.0a1",

    packages=["pypb"],

    install_requires=[
        'logbook',
        'gevent',
        'python-daemon',
        'setproctitle',
        'msgpack-python',
    ],

    author="Parantapa Bhattacharya",
    author_email="pb [at] parantapa [dot] net",

    description="Misc modules by and for PB.",
)
