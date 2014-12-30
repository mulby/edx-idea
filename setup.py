import os
from setuptools import setup


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="edx_idea",
    version="0.1.0",
    author="Gabe Mulley",
    author_email="gabe@edx.org",
    description="A data processing pipeline management tool.",
    license="AGPLv3",
    url="http://code.edx.org",
    packages=[
        'edx',
        'edx.idea',
        'edx.idea.common',
        'edx.idea.spark'
    ],
    long_description=read('README.md'),
    install_requires=[
        'PyYAML==3.10',
        'stevedore==1.1.0',
        'jinja2'
    ],
    extras_require={
        'ipython': [
            'ipython',
            'pyzmq',
            'jinja2',
            'tornado'
        ],
        'examples': [
            'python-dateutil',
        ],
    },
    entry_points={
        'console_scripts': [
            'idea = edx.idea.executor:main',
        ],
        'edx.idea.engine': [
            'spark = edx.idea.spark.engine:SparkEngine',
        ]
    }
)
