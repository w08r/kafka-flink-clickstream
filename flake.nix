{
  description = "A java dev flake";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  inputs.flake-utils.url = "github:numtide/flake-utils";

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      (with nixpkgs.legacyPackages.${system};
        {
          devShells = rec {
            default = mkShell {
              packages = [
                jdk22
                maven
                google-java-format
                graalvm-ce
                gcc
              ];
            };
          };
        }
      )
    );
}
