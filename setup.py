from setuptools import setup, find_packages

setup(
    name='extensions_project',
    version='1.0',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'arrow==0.10.0',
        'click==7',
        'numpy==1.16.2',
        'pyspark==2.4.0',
        'python_moztelemetry==0.10.5',
        'scipy==1.2.1',
    ],
)
