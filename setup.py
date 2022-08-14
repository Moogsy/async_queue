import setuptools

with open("README.md", "r") as f:
    long_description = f.read()

setuptools.setup(
    name="async_queues",
    version="0.0.2",
    author="An Akiyama",
    author_email="saphielle.akiyama@gmail.com",
    description="An attempt to re-create / extend asyncio's current queues",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Moogsy/async_queue",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)