import os

from setuptools import find_packages, setup

# with open(os.path.join(os.path.dirname(__file__), 'README.rst')) as readme:
#     README = readme.read()
README = 'test'

setup(
    name='celery-clusterwatch',
    version='0.1',
    packages=find_packages(),
    include_package_data=True,
    license='MIT License',
    description='Send Celery cluster statistics to AWS CloudWatch',
    long_description=README,
    url='https://github.com/qwyk/celery-clusterwatch',
    author='Qwyk B.V.',
    author_email='martyn@qywk.io',
    keywords=['celery', 'aws', 'cloudwatch'],
    entry_points={
        'celery.commands': [
            'clusterwatch = celery_clusterwatch.__main__:ClusterWatchCommand'
        ]
    },
    install_requires=[
        'boto3'        
    ],
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.6'
    ]
)
