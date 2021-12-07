with import <nixpkgs> {};
stdenv.mkDerivation {
  name = "Pijul";
  buildInputs = with pkgs; [
    zstd
    libsodium
    openssl
    pkgconfig
    libiconv
    xxHash
  ] ++ lib.optionals stdenv.isDarwin
    (with darwin.apple_sdk.frameworks; [
      CoreServices
      Security
      SystemConfiguration
    ]);
}
