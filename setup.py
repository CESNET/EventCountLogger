import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="event_count_logger",
    version="1.0.0",
    author="Václav Bartoš",
    author_email="bartos@cesnet.cz",
    description="Count number of events per time interval(s) in a distributed system using shared counters in Redis.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/CESNET/EventCountLogger",
    license="BSD",
    #packages=setuptools.find_packages(),
    py_modules=["event_count_logger"],
    scripts=["bin/ecl_master", "bin/ecl_log_event", "bin/ecl_reader"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Intended Audience :: Developers",
        "Topic :: Software Development",
        "Topic :: System :: Logging",
        "Typing :: Typed",
        "Development Status :: 4 - Beta",
    ],
    python_requires='>=3.6',
)