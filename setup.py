import setuptools

with open("README.md", "r") as f:
    long_description = f.read()

setuptools.setup(
    name='analytoolz',
    version='0.0.1',
    author='Mak Shimizu',
    author_email='mak@fish.razor.jp',
    description='Utilities for Google Analytics and Google Cloud Platform.',
    long_description=long_description,
    long_description_content_type='ext/markdown',
    packages=setuptools.find_packages(),
    classifiers=(
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ),
)
