import nox
import sys

# Run everything but 'dist' by default
nox.options.keywords = "not dist"


@nox.session(python=["3.6", "3.7", "3.8", "3.9"])
def tests(session):
    env = None
    if session.python == "3.6":
        if not sys.stdout.encoding.lower().startswith('utf'):
            env = {"PYTHONIOENCODING": "UTF8"}
    session.install(".[test]")
    session.run("pytest", "-x", "--log-level=debug", *session.posargs, env=env)


@nox.session
def format(session):
    session.install("black")
    session.run(
        "black",
        "--target-version=py36",
        "--line-length=79",
        "--skip-string-normalization",
        ".",
    )


@nox.session
def lint(session):
    session.install("flake8")
    session.run("flake8")


@nox.session
def dist(session):
    session.install(".[dist]")
    session.run("check-manifest")
    session.run("python", "setup.py", "bdist_wheel", "sdist")
    session.run("twine", "upload", "dist/*")
    print("*** Don't forget to tag and push!")
