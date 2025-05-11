from setuptools import setup, find_packages

setup(
    name="smartabasepy",
    version="0.1",
    packages=find_packages(),
    install_requires=["requests", "pandas", "keyring"],
    author="Brandon Yach", 
    author_email="yachb35@gmail.com",
    description="A Python library for interacting with Smartabase API",
    license="MIT",
    url="https://github.com/brandonyach/smartabasepy", 
)