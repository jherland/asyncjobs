from setuptools import setup

with open("README.md") as f:
    long_description = f.read()

setup(
    name="asyncjobs",
    version="0.2.0",
    description="Asynchronous job scheduler",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/jherland/asyncjobs",
    author="Johan Herland",
    author_email="johan@herland.net",
    packages=["asyncjobs"],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Framework :: AsyncIO",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Topic :: Software Development :: Build Tools",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Utilities",
    ],
    keywords="async asyncio job scheduler",
    python_requires=">=3.6",
    install_requires=[],
    extras_require={
        "dev": ["black", "flake8", "nox"],
        "dist": ["check-manifest", "twine", "wheel"],
        "plot": ["numpy", "plotly"],
        "test": ["pytest>=5.4.0", "pytest-asyncio", "requests"],
    },
)
