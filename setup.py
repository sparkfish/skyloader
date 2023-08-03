from setuptools import setup, find_packages


def parse_requirements(filename):
    with open(filename, encoding='utf-8') as f:
        lines = f.read().strip().split('\n')
    return [line for line in lines if line and not line.startswith('#')]


required = parse_requirements('requirements.txt')

setup(
    name="skyloader",
    version="0.1",
    description="Loads Excel, CSV and other tabular data files from a cloud drive provider into a target database table.",
    author="Sparkfish LLC",
    author_email="packages@sparkfish.com",
    packages=find_packages(),
    install_requires=required
)