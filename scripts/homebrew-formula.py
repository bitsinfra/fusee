#!/usr/bin/env python3
"""Writes a Homebrew formula for the fusee CLI over a release's binaries
(fusee-darwin-arm64, fusee-darwin-x64, fusee-linux-x64-musl,
fusee-linux-arm64-musl in <dir>), with their SHA-256 sums, for a tap:

    python3 scripts/homebrew-formula.py 1.0.0 out > fusee.rb
"""
import hashlib
import os
import sys

REPO = "https://github.com/bitsinfra/fusee"


def sha(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> int:
    version, d = sys.argv[1], sys.argv[2]
    url = f"{REPO}/releases/download/v{version}"
    s = {p: sha(os.path.join(d, f"fusee-{p}")) for p in
         ("darwin-arm64", "darwin-x64", "linux-x64-musl", "linux-arm64-musl")}
    print(f'''class Fusee < Formula
  desc "Durable workflows in your process, or as a server: the fusee CLI"
  homepage "{REPO}"
  version "{version}"
  license any_of: ["MIT", "Apache-2.0"]

  on_macos do
    on_arm do
      url "{url}/fusee-darwin-arm64"
      sha256 "{s["darwin-arm64"]}"
    end
    on_intel do
      url "{url}/fusee-darwin-x64"
      sha256 "{s["darwin-x64"]}"
    end
  end

  on_linux do
    on_arm do
      url "{url}/fusee-linux-arm64-musl"
      sha256 "{s["linux-arm64-musl"]}"
    end
    on_intel do
      url "{url}/fusee-linux-x64-musl"
      sha256 "{s["linux-x64-musl"]}"
    end
  end

  def install
    bin.install Dir["fusee-*"].first => "fusee"
  end

  test do
    assert_match version.to_s, shell_output("#{{bin}}/fusee version")
  end
end''')
    return 0


if __name__ == "__main__":
    sys.exit(main())
