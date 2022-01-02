{
  description = "pijul, the sound distributed version control system";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-21.11";
    rust-overlay.url = "github:oxalica/rust-overlay";
  };

  outputs =
    { self
    , nixpkgs
    , rust-overlay
    , ...
    } @ inputs:
    let
      nameValuePair = name: value: { inherit name value; };
      genAttrs = names: f: builtins.listToAttrs (map (n: nameValuePair n (f n)) names);
      allSystems = [ "x86_64-linux" "aarch64-linux" "i686-linux" "x86_64-darwin" ];
      forAllSystems = f: genAttrs allSystems (system: f system);

      rustOverlay = final: prev:
        let
          rustChannel = prev.rust-bin.stable."1.56.0";
        in {
          inherit rustChannel;
          rustc = rustChannel.minimal;
        };
    in {
      devShell = forAllSystems (system:
        let
          rustDevOverlay = final: prev: {
            # rust-analyzer needs core source
            rustc-with-src = prev.rustc.override { extensions = [ "rust-src" ]; };
          };
          pkgs = import nixpkgs {
            inherit system;
            overlays = [
              (import rust-overlay)
              rustOverlay
              rustDevOverlay
            ];
          };
        in pkgs.mkShell {
          name = "pijul";

          inputsFrom = [ self.packages.${system}.pijul-git ];

          # Eventually crate2nix will provide a devShell that includes transitive dependencies for us.
          # https://github.com/kolloch/crate2nix/issues/111
          packages = with pkgs; [
            pkg-config
            clang
            openssl

            rust-analyzer rustc-with-src
            rustfmt
            crate2nix
          ];

          LIBCLANG_PATH = "${pkgs.llvmPackages.libclang}/lib";
        });

      packages = forAllSystems
        (system:
          let
            pkgs = import nixpkgs {
              inherit system;
              overlays = [
                (import rust-overlay)
                rustOverlay
              ];
            };
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
                        xxHash
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
          in {
            inherit pijul;
            pijul-git = pijul.override { features = [ "git" ]; };
          });

      defaultPackage = forAllSystems (system: self.packages.${system}.pijul);
    };
}
