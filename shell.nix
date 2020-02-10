# Run nix-shell without arguments to enter an environment with all the
# following stuff in place.

with import <nixpkgs> {};

stdenv.mkDerivation {
  name = "asyncjobs-env";
  buildInputs = [
    # Python requirements (enough to get a virtualenv going).
    python37
  ];
  src = null;
  shellHook = ''
    # Allow the use of wheels.
    unset SOURCE_DATE_EPOCH

    # Setup up virtualenv for development
    python -m venv --clear .venv
    source .venv/bin/activate

    # Install project deps
    pip install -r requirements-dev.txt
  '';
}
