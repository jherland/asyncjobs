# Run nix-shell without arguments to enter an environment with all the
# project dependencies in place.
{
  pkgs ? import (builtins.fetchGit {
    url = "https://github.com/NixOS/nixpkgs-channels/";
    ref = "nixos-20.03";
  }) {}
}:

let
  pythonPackages = pkgs.python38Packages;
in pkgs.mkShell {
  venvDir = "./.venv";
  buildInputs = [
    pkgs.git
    pythonPackages.python
    pythonPackages.venvShellHook
  ];
  postShellHook = ''
    pip install -e .\[dev,plot,examples\]
  '';
}
