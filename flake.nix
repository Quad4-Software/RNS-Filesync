{
  description = "RNS FileSync - Peer-to-Peer File Synchronization over Reticulum";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
        };

        rns = pkgs.python3Packages.buildPythonPackage rec {
          pname = "rns";
          version = "1.0.4";
          format = "pyproject";

          src = pkgs.python3Packages.fetchPypi {
            inherit pname version;
            hash = "sha256-5wZnp2f+Ujurjn6gYnRHJYxOZ2O3dW+7pQxlVtu4Q5k=";
          };

          nativeBuildInputs = with pkgs.python3Packages; [
            setuptools
            wheel
          ];

          propagatedBuildInputs = with pkgs.python3Packages; [
            cryptography
            pyserial
            netifaces
          ];

          doCheck = false;
        };

        pythonEnv = pkgs.python3.withPackages (ps: with ps; [
          rns
        ]);

        rnsFilesync = pkgs.writeShellApplication {
          name = "rns-filesync";
          runtimeInputs = [ pythonEnv ];
          text = ''
            exec ${pythonEnv}/bin/python ${./rns_filesync.py} "$@"
          '';
        };
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = [ pythonEnv ];

          shellHook = ''
            echo "RNS FileSync development environment"
            echo "Python: $(${pythonEnv}/bin/python --version)"
            echo "RNS is available in the Python environment"
            echo "Run: python rns_filesync.py -d <directory>"
          '';
        };

        packages.default = rnsFilesync;

        apps.default = {
          type = "app";
          program = "${rnsFilesync}/bin/rns-filesync";
        };
      }
    );
}

