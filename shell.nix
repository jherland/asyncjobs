# Run nix-shell without arguments to enter an environment with all the
# following stuff in place.

with import <nixpkgs> {};

stdenv.mkDerivation {
  name = "asyncio_builder-environment";
  buildInputs = [
    # Python requirements (enough to get a virtualenv going).
    python37
    pipenv
  ];
  src = null;
  shellHook = ''
    # Allow the use of wheels.
    SOURCE_DATE_EPOCH=$(date +%s)

    # Prevent "ModuleNotFoundError: No module named 'pip._internal.main'"
    export PYTHONPATH=$(pipenv --venv)/lib/python3.7/site-packages/:$PYTHONPATH

    # Install Pipfile dependencies
    ${pipenv}/bin/pipenv install --dev
    ${pipenv}/bin/pipenv shell
  '';
}
