from setuptools import find_packages, setup

extra_all = ["orjson"]
extra_kafka = ["confluent-kafka", "python-snappy"]

setup(
    name="kafka_utils",
    version="1.0.0",
    description="A toolkit leveraging the power of the confluent-kafka library provides a comprehensive set of utilities for seamless management and interaction with Apache Kafka clusters.",
    url="https://github.com/NikolaS2891/kafka-utils",
    author="Nikola Stankovic",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.10",
        "Operating System :: OS Independent",
    ],
    install_requires=[*extra_all, *extra_kafka],
)
