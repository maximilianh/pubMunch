from setuptools import setup, find_packages

version = '0.1.3'

REQUIREMENTS = [
    'beautifulsoup',
    'requests'
]

setup(
    name='incapsula-cracker',
    version=version,
    packages=find_packages(),
    url='https://github.com/ziplokk1/incapsula-cracker',
    license='LICENSE.txt',
    author='Mark Sanders',
    author_email='sdscdeveloper@gmail.com',
    install_requires=REQUIREMENTS,
    description='A way to bypass incapsula robot checks when using requests or scrapy.',
    include_package_data=True
)
