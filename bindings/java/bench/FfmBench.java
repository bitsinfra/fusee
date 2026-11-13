package dev.fusee;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;

/**
 * Per-call cost of JNI (the SDK's binding) against the Foreign Function &
 * Memory API (a preview in Java 21) for the same C functions in the same
 * library: no arguments, primitives only, and a string argument with two
 * out-pointers. Not part of the Maven build (it needs --enable-preview on
 * Java 21, or Java 22 without Linker.Option.isTrivial). From bindings/java,
 * after mvn compile:
 *
 * <pre>
 * javac --release 21 --enable-preview -cp target/classes -d target/ffm bench/FfmBench.java
 * java --enable-preview --enable-native-access=ALL-UNNAMED \
 *     -Dlib=target/classes/dev/fusee/native/darwin-arm64/libfusee_jni.dylib \
 *     -cp target/classes:target/ffm dev.fusee.FfmBench
 * </pre>
 */
public final class FfmBench {
    static final int N = 2_000_000;

    public static void main(String[] args) throws Throwable {
        Native.init();
        String lib = System.getProperty("lib");
        long h = Native.open("memory".getBytes(), null, null, -1, new byte[0][], 1, false, false, -1, -1, -1, -1, -1,
                null, new byte[0][], new byte[0][], 0);
        Native.limit(h, "bench".getBytes(), 4, 0);
        Linker linker = Linker.nativeLinker();
        Arena arena = Arena.ofShared();
        SymbolLookup look = SymbolLookup.libraryLookup(Path.of(lib), arena);
        MethodHandle abi = linker.downcallHandle(look.find("fusee_abi_version").orElseThrow(),
                FunctionDescriptor.of(ValueLayout.JAVA_INT));
        MethodHandle abiTrivial = linker.downcallHandle(look.find("fusee_abi_version").orElseThrow(),
                FunctionDescriptor.of(ValueLayout.JAVA_INT), Linker.Option.isTrivial());
        MethodHandle codeState = linker.downcallHandle(look.find("fusee_engine_code_state").orElseThrow(),
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_INT,
                        ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));
        StructLayout str = MemoryLayout.structLayout(ValueLayout.ADDRESS.withName("ptr"),
                ValueLayout.JAVA_LONG.withName("len"));
        MethodHandle usage = linker.downcallHandle(look.find("fusee_engine_limit_usage").orElseThrow(),
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, str, ValueLayout.ADDRESS,
                        ValueLayout.ADDRESS));
        // The JNI handle's first field is the fusee_engine pointer.
        MemorySegment engine = MemorySegment.ofAddress(h).reinterpret(8).get(ValueLayout.ADDRESS, 0);
        MemorySegment outs = arena.allocate(32, 8);
        MemorySegment name = arena.allocate(64, 8);
        MemorySegment fstr = arena.allocate(str);
        byte[] nameBytes = "bench".getBytes(StandardCharsets.UTF_8);
        long[] out = new long[2];
        {
            MemorySegment.copy(nameBytes, 0, name, ValueLayout.JAVA_BYTE, 0, nameBytes.length);
            fstr.set(ValueLayout.ADDRESS, 0, name);
            fstr.set(ValueLayout.JAVA_LONG, 8, nameBytes.length);
            int rc = (int) usage.invokeExact(engine, fstr, outs, outs.asSlice(8));
            Native.limitUsage(h, nameBytes, out);
            System.out.println("ffm rc " + rc + " held " + outs.get(ValueLayout.JAVA_INT, 0) + "; jni held " + out[0]);
            MemorySegment.copy("nope".getBytes(), 0, name, ValueLayout.JAVA_BYTE, 0, 4);
            fstr.set(ValueLayout.JAVA_LONG, 8, 4);
            rc = (int) usage.invokeExact(engine, fstr, outs, outs.asSlice(8));
            System.out.println("ffm rc for an unknown limit " + rc);
        }
        int rounds = 5;
        double[][] r = new double[7][rounds];
        for (int round = 0; round < rounds; round++) {
            long t = System.nanoTime();
            int sink = 0;
            for (int i = 0; i < N; i++) sink += Native.abiVersion();
            r[0][round] = (System.nanoTime() - t) / (double) N;
            t = System.nanoTime();
            for (int i = 0; i < N; i++) sink += (int) abi.invokeExact();
            r[1][round] = (System.nanoTime() - t) / (double) N;
            t = System.nanoTime();
            for (int i = 0; i < N; i++) sink += (int) abiTrivial.invokeExact();
            r[2][round] = (System.nanoTime() - t) / (double) N;
            t = System.nanoTime();
            for (int i = 0; i < N; i++) sink += Native.codeState(h, 0, 1);
            r[3][round] = (System.nanoTime() - t) / (double) N;
            t = System.nanoTime();
            for (int i = 0; i < N; i++) sink += (int) codeState.invokeExact(engine, 0, 1L, outs);
            r[4][round] = (System.nanoTime() - t) / (double) N;
            t = System.nanoTime();
            for (int i = 0; i < N; i++) {
                Native.limitUsage(h, nameBytes, out);
                sink += (int) out[0];
            }
            r[5][round] = (System.nanoTime() - t) / (double) N;
            t = System.nanoTime();
            for (int i = 0; i < N; i++) {
                // As a binding would: copy the bytes in, call, read the outputs.
                MemorySegment.copy(nameBytes, 0, name, ValueLayout.JAVA_BYTE, 0, nameBytes.length);
                fstr.set(ValueLayout.ADDRESS, 0, name);
                fstr.set(ValueLayout.JAVA_LONG, 8, nameBytes.length);
                sink += (int) usage.invokeExact(engine, fstr, outs, outs.asSlice(8));
                sink += outs.get(ValueLayout.JAVA_INT, 0);
            }
            r[6][round] = (System.nanoTime() - t) / (double) N;
            if (sink == 42) System.out.println();
        }
        String[] names = {"JNI   fusee_abi_version()", "FFM   fusee_abi_version()", "FFM   fusee_abi_version() trivial",
            "JNI   fusee_engine_code_state(e, u32, u64, *u32)", "FFM   fusee_engine_code_state(e, u32, u64, *u32)",
            "JNI   fusee_engine_limit_usage(e, str, *u32, *u64)", "FFM   fusee_engine_limit_usage(e, str, *u32, *u64)"};
        for (int i = 0; i < names.length; i++) {
            double[] v = r[i].clone();
            Arrays.sort(v);
            System.out.printf("%-55s median %.1f ns/call (best %.1f)%n", names[i], v[rounds / 2], v[0]);
        }
        Native.close(h);
    }
}
