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

        pythonEnv = pkgs.python3.withPackages (ps: with ps; [
          pip
        ]);

        rnsFilesync = pkgs.stdenv.mkDerivation {
          pname = "rns-filesync";
          version = "1.0.0";
          src = ./.;

          buildInputs = [ pythonEnv ];

          installPhase = ''
            mkdir -p $out/bin
            mkdir -p $out/lib

            cp ${./rns_filesync.py} $out/lib/rns_filesync.py
            chmod +x $out/lib/rns_filesync.py

            cat > $out/bin/rns-filesync <<EOF
            #!${pkgs.bash}/bin/bash
            export PATH="${pythonEnv}/bin:\$PATH"
            ${pythonEnv}/bin/pip install --quiet --user rns 2>/dev/null || \
              ${pythonEnv}/bin/pip install --quiet rns 2>/dev/null || true
            exec ${pythonEnv}/bin/python $out/lib/rns_filesync.py "\$@"
            EOF

            chmod +x $out/bin/rns-filesync
          '';
        };
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = [
            pythonEnv
            pkgs.python3Packages.pip
          ];

          shellHook = ''
            echo "RNS FileSync development environment"
            echo "Python: $(${pythonEnv}/bin/python --version)"
            ${pythonEnv}/bin/pip install --quiet --user rns 2>/dev/null || \
              ${pythonEnv}/bin/pip install --quiet rns 2>/dev/null || \
              echo "Note: Install rns with: pip install rns"
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

