# Run nix-shell without arguments to enter an environment with all the
# project dependencies in place.

with import <nixpkgs> {};

stdenv.mkDerivation {
  name = "asyncjobs-env";
  buildInputs = [
    # Python requirements (enough to get a virtualenv going).
    python38
  ];
  src = null;
  shellHook = ''
    # Allow the use of wheels.
    unset SOURCE_DATE_EPOCH

    # Setup up virtualenv for development
    python -m venv --clear .venv
    source .venv/bin/activate

    # Install project deps
    pip install -e .\[dev\] -e .\[plot\]
  '';
}
