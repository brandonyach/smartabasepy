from setuptools import setup, find_packages, os

setup(
    name="smartabasepy",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "requests", 
        "requests-toolbelt",
        "pandas", 
        "keyring",
        "os",
        "tqdm",
        "typing",
        "datetime",
        "hashlib",
        "mimetype"
        ],
    author="Brandon Yach", 
    author_email="yachb35@gmail.com",
    description="A Python library for interacting with the Teamworks AMS API",
    long_description=open("README.md").read() if os.path.exists("README.md") else "",
    long_description_content_type="text/markdown",
    license="MIT",
    url="https://github.com/brandonyach/smartabasepy", 
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
    ]
)