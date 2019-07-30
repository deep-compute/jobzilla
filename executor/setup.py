from setuptools import setup, find_packages

version = 0.1
setup(
    name="executor",
    version=version,
    description="Request and execute jobs",
    keyword="job processing platform",
    author="DeepCompute, LLC",
    author_email="contact@deepcompute.com",
    url="https://github.com/deep-compute/job_processing_platform",
    download_url="https://github.com/deep-compute/job_processing_platform/tarball/%s"
    % version,
    install_requires=["docker", "basescript", "kwikapi", "typing"],
    package_dir={"executor": "executor"},
    packages=find_packages("."),
    test_suite="test.test_suite",
    entry_points={"console_scripts": ["executor = executor:main"]},
)
