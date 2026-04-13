from setuptools import find_packages, setup

with open("requirements.txt") as f:
    install_requires = [line.strip() for line in f if line.strip() and not line.startswith("#")]

setup(
    name="call_intelligence",
    version="1.0.0",
    description="Call Intelligence + Patient 360 CRM for Frappe/ERPNext",
    author="",
    author_email="",
    packages=find_packages(),
    zip_safe=False,
    include_package_data=True,
    install_requires=install_requires,
)
