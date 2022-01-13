with import <nixpkgs> {};

let zstd14 = stdenv.mkDerivation rec {
      pname = "zstd";
      version = "1.4.9";

      src = fetchFromGitHub {
        owner = "facebook";
        repo = "zstd";
        rev = "v${version}";
        sha256 = "0icc0x89c35rq5bxd4d241vqxnz2i1qj2wwy01xls63p0z93brj7";
      };

      nativeBuildInputs = [ cmake ];
      cmakeFlags = lib.attrsets.mapAttrsToList
        (name: value: "-DZSTD_${name}:BOOL=${if value then "ON" else "OFF"}") {
          BUILD_SHARED = true;
          BUILD_STATIC = true;
          BUILD_CONTRIB = true;
          PROGRAMS_LINK_SHARED = false;
          LEGACY_SUPPORT = false;
          BUILD_TESTS = false;
        };
      cmakeDir = "../build/cmake";
      dontUseCmakeBuildDir = true;
      preConfigure = ''
    mkdir -p build_ && cd $_
  '';
    };
in
stdenv.mkDerivation {
  name = "Pijul";
  buildInputs = with pkgs; [
    zstd14
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
