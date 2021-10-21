{
  description = "pijul, the sound distributed version control system";

  inputs.nixpkgs.url = "github:nixos/nixpkgs/nixos-21.05";
  inputs.mozilla = { url = "github:mozilla/nixpkgs-mozilla"; flake = false; };

  outputs =
    { self
    , nixpkgs
    , mozilla
    , ...
    } @ inputs:
    let
      nameValuePair = name: value: { inherit name value; };
      genAttrs = names: f: builtins.listToAttrs (map (n: nameValuePair n (f n)) names);
      allSystems = [ "x86_64-linux" "aarch64-linux" "i686-linux" "x86_64-darwin" ];

      rustOverlay = final: prev:
        let
          rustChannel = prev.rustChannelOf {
            channel = "1.54.0";
            sha256 = "sha256-2NfCJiH3wk7sR1XlRf8+IZfY3S9sYKdL8TpMqk82Bq0=";
          };
        in
        {
          inherit rustChannel;
          rustc = rustChannel.rust;
          cargo = rustChannel.rust;
        };

      forAllSystems = f: genAttrs allSystems (system: f {
        inherit system;
        pkgs = import nixpkgs {
          inherit system;
          overlays = [
            (import "${mozilla}/rust-overlay.nix")
            rustOverlay
          ];
        };
      });
    in
    {
      devShell = forAllSystems ({ system, pkgs, ... }:
        pkgs.mkShell {
          name = "pijul";

          inputsFrom = [ self.packages.${system}.pijul-git ];

          # Eventually crate2nix will provide a devShell that includes transitive dependencies for us.
          # https://github.com/kolloch/crate2nix/issues/111
          buildInputs = with pkgs; [
            pkg-config
            clang
            openssl

            # rustChannel.rust provides tools like clippy, rustfmt, cargo,
            # rust-analyzer, rustc, and more.
            (rustChannel.rust.override { extensions = [ "rust-src" ]; })
            crate2nix
          ];

          LIBCLANG_PATH = "${pkgs.llvmPackages.libclang}/lib";
        });

      packages = forAllSystems
        ({ system, pkgs, ... }:
          let
            pijul =
              let
                cargoNix = import ./Cargo.nix {
                  inherit pkgs;
                  defaultCrateOverrides = pkgs.defaultCrateOverrides // {
                    zstd-seekable = { ... }: {
                      nativeBuildInputs = [ pkgs.clang ]
                        ++ pkgs.lib.optionals pkgs.stdenv.isDarwin [ pkgs.xcbuild ];
                      LIBCLANG_PATH = "${pkgs.llvmPackages.libclang}/lib";
                    };

                    blake3 = attr: {
                      nativeBuildInputs = pkgs.lib.optionals pkgs.stdenv.isDarwin [ pkgs.xcbuild ];
                    };

                    pijul = { ... }: {
                      buildInputs = with pkgs; [
                        zstd
                        xxHash
                        libsodium
                        libiconv
                      ] ++ lib.optionals stdenv.isDarwin (
                        [ openssl ]
                        ++ (with darwin.apple_sdk.frameworks; [
                          CoreServices
                          Security
                          SystemConfiguration
                      ]));
                    };
                  };
                };
              in
              cargoNix.workspaceMembers.pijul.build;
          in
          {
            inherit pijul;
            pijul-git = pijul.override { features = [ "git" ]; };
          });

      defaultPackage = forAllSystems ({ system, ... }: self.packages.${system}.pijul);
    };
}
