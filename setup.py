import pathlib
from setuptools import setup

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

# This call to setup() does all the work
setup(
    name="QueryMaker",
    version="0.2.3",
    description="MySql query builder and executor, based on aiomysql",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/ryazantseff/mysql-querymaker/archive/0.2.tar.gz",
    author="Maxim Ryazantsev",
    author_email="maxim.ryazancev@gmail.com",
    license="MIT",
    keywords = ['Mysql', 'Sql', 'Query', 'async'],   
    install_requires=[
        'asyncio',
        'aiomysql',
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',      
        'Intended Audience :: Developers',      
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',   
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
)
