from setuptools import find_packages, setup

if __name__ == "__main__":
    setup(
        name="sirius_datateam",
        packages=find_packages(exclude=["sirius_datateam_tests"]),
        install_requires=[
            "dagster",
            "dagster_aws",
        ],
    )
