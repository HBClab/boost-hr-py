{
  description = "Unified Python + Rust dev environment (venv + rust-toolchain)";

  inputs = {
    nixpkgs.url = "https://flakehub.com/f/NixOS/nixpkgs/0.1.*.tar.gz";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, rust-overlay }:
    let
      supportedSystems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];

      forEachSupportedSystem = f:
        nixpkgs.lib.genAttrs supportedSystems (system:
          f {
            pkgs = import nixpkgs {
              inherit system;
              overlays = [ rust-overlay.overlays.default self.overlays.default ];
            };
          });

      # Change this to bump Python minor (delete .venv after changing!)
      pyVersion = "3.13";

      concatMajorMinor = v: pkgs:
        pkgs.lib.pipe v [
          pkgs.lib.versions.splitVersion
          (pkgs.lib.sublist 0 2)
          pkgs.lib.concatStrings
        ];
    in
    {
      overlays.default = final: prev: {
        # Prefer rust-toolchain.toml if present; otherwise latest stable with src+fmt.
        rustToolchain =
          let rb = prev.rust-bin; in
          if builtins.pathExists ./rust-toolchain.toml then
            rb.fromRustupToolchainFile ./rust-toolchain.toml
          else if builtins.pathExists ./rust-toolchain then
            rb.fromRustupToolchainFile ./rust-toolchain
          else
            rb.stable.latest.default.override {
              extensions = [ "rust-src" "rustfmt" ];
            };
      };

      devShells = forEachSupportedSystem ({ pkgs }:
        let
          python = pkgs."python${concatMajorMinor pyVersion pkgs}";
        in
        {
          # single, unified shell
          default = pkgs.mkShell {
            venvDir = ".venv";

            packages = with pkgs; [
              # ---- Python toolchain + libs ----
              python.pkgs.venvShellHook
              python.pkgs.pip
              python.pkgs.pandas
              python.pkgs.numpy
              python.pkgs.openpyxl
              python.pkgs.matplotlib
              python.pkgs.seaborn
              python.pkgs.plotly
              python.pkgs.requests
              python.pkgs.httpx
              python.pkgs.jupyterlab
              python.pkgs.ipython
              python.pkgs.scipy
              python.pkgs.pyyaml
              python.pkgs.pytest

              # ---- Rust toolchain + helpers ----
              rustToolchain
              rust-analyzer
              cargo-deny
              cargo-edit
              cargo-watch

              # ---- Common build deps (openssl for crates, pkg-config, git) ----
              openssl
              pkg-config
              git
            ] ++ (if pkgs.stdenv.isDarwin then [ pkgs.libiconv ] else []);

            # rust-analyzer needs RUST_SRC_PATH
            env.RUST_SRC_PATH = "${pkgs.rustToolchain}/lib/rustlib/src/rust/library";

            # Keep your venv version warning helper
            postShellHook = ''
              venvVersionWarn() {
                local venvVersion
                if [[ -x "$venvDir/bin/python" ]]; then
                  venvVersion="$("$venvDir/bin/python" -c 'import platform; print(platform.python_version())')"
                  if [[ "$venvVersion" != "${python.version}" ]]; then
                    cat <<EOF
Warning: Python version mismatch: [$venvVersion (venv)] != [${python.version}]
Delete '$venvDir' and reload to rebuild for version ${python.version}
EOF
                  fi
                fi
              }
              venvVersionWarn
            '';
          };
        });
    };
}

