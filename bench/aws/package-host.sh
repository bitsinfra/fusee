# Exercise registry-formatted packages on a disposable native Linux host.
set -euo pipefail
out="$FUSEE_RESULTS/packages"
mkdir -p "$out"
case $(uname -m) in x86_64) rustarch=x86_64; npmarch=x64 ;; aarch64) rustarch=aarch64; npmarch=arm64 ;; *) exit 2 ;; esac
for abi in gnu musl; do
    target="$rustarch-unknown-linux-$abi"
    platform="linux-$npmarch-$abi"
    common_pkg=(--rm -w /src -v /opt/fusee:/src:ro -v "$out:/out" -v "fusee-package-$abi:/build" -e CARGO_TARGET_DIR=/build -e TARGET="$target" -e PLATFORM="$platform")
    if [[ $abi == gnu ]]; then
        img=ghcr.io/pyo3/maturin:v1.16.0
        docker run "${common_pkg[@]}" --entrypoint bash "$img" -euc '
            rustc --version; maturin --version
            cargo build --locked --release --target "$TARGET" -p fusee-node -p fusee-cli
            cp /build/$TARGET/release/libfusee_node.so /out/$PLATFORM.node
            cp /build/$TARGET/release/fusee /out/$PLATFORM-server
            strip --strip-debug /out/$PLATFORM.node /out/$PLATFORM-server
            maturin build --locked --release --manifest-path bindings/python/Cargo.toml --target "$TARGET" --compatibility manylinux_2_28 --out /out
        '
        pyimage=python:3.13-bookworm
        nodeimage=node:24.14.0-bookworm-slim
        wheeltag=manylinux
    else
        img=rust:1.95-alpine3.23
        docker run "${common_pkg[@]}" --entrypoint sh "$img" -euc '
            apk add --no-cache python3 py3-pip patchelf build-base
            python3 -m venv /tmp/p; /tmp/p/bin/pip install maturin==1.16.0
            cargo build --locked --release --target "$TARGET" -p fusee-cli
            export RUSTFLAGS="-C target-feature=-crt-static"
            cargo build --locked --release --target "$TARGET" -p fusee-node
            cp /build/$TARGET/release/libfusee_node.so /out/$PLATFORM.node
            cp /build/$TARGET/release/fusee /out/$PLATFORM-server
            strip --strip-debug /out/$PLATFORM.node /out/$PLATFORM-server
            /tmp/p/bin/maturin build --locked --release --manifest-path bindings/python/Cargo.toml --target "$TARGET" --compatibility musllinux_1_2 --out /out
        '
        pyimage=python:3.13-alpine3.23
        nodeimage=node:24.14.0-alpine3.23
        wheeltag=musllinux
    fi
    docker run --rm -v "$out:/packages:ro" -e PLATFORM="$platform" -e WHEELTAG="$wheeltag" "$pyimage" sh -euc '
        python -m pip install --disable-pip-version-check --no-index /packages/*$WHEELTAG*.whl
        /packages/$PLATFORM-server version
        python -c "import fusee,asyncio;exec(\"async def main():\\n e=await fusee.Engine.open(durability=\\\"memory\\\")\\n @e.workflow(\\\"smoke\\\")\\n async def flow(ctx,n):return await ctx.step(\\\"inc\\\",lambda:n+1)\\n assert await (await flow.start(\\\"id\\\",41)).result()==42\\n await e.close()\");asyncio.run(main())"
    '
    docker run --rm -v /opt/fusee/bindings/node:/sdk:ro -v "$out:/packages" -e PLATFORM="$platform" "$nodeimage" sh -euc '
        cp -r /sdk /tmp/fusee-node; cd /tmp/fusee-node
        npm install --omit=optional --ignore-scripts --no-audit --no-fund
        node_modules/.bin/tsc -p tsconfig.json
        node scripts/package-platform.mjs "$PLATFORM" "/packages/$PLATFORM.node" /tmp/plat
        npm pack /tmp/plat --pack-destination /packages --silent
        npm pack . --pack-destination /packages --silent
        mkdir /tmp/consumer; cd /tmp/consumer
        npm install --offline --ignore-scripts --no-audit /packages/fusee-0*.tgz /packages/fusee-$PLATFORM-*.tgz
        node --input-type=module -e '\''import {Engine} from "fusee";const e=await Engine.open({durability:"memory"});const w=e.workflow("smoke",(ctx,n)=>ctx.step("add",()=>n+1));if(await(await w.start("id",41)).result()!==42)throw Error("bad result");await e.close();'\''
    '
    sha256sum "$out/$platform.node" >"$FUSEE_RESULTS/$platform.sha256"
done
