package dev.fusee;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Locale;

/**
 * Loads libfusee_jni: from the path in the system property
 * {@code fusee.library} if set; else from the class path at
 * {@code dev/fusee/native/<platform>/libfusee_jni.<dylib|so>} (copied to a
 * temporary file first when it is inside a jar); else from
 * {@code java.library.path}. Platforms: darwin-arm64, darwin-x86_64,
 * linux-x86_64, linux-aarch64.
 */
final class NativeLoader {
    private NativeLoader() {}

    private static boolean loaded;

    static synchronized void load() {
        if (loaded) return;
        String explicit = System.getProperty("fusee.library");
        if (explicit != null && !explicit.isEmpty()) {
            System.load(new File(explicit).getAbsolutePath());
            loaded = true;
            return;
        }
        String platform = platform();
        String name = System.mapLibraryName("fusee_jni");
        String resource = "dev/fusee/native/" + platform + "/" + name;
        URL url = NativeLoader.class.getClassLoader() == null ? ClassLoader.getSystemResource(resource)
                : NativeLoader.class.getClassLoader().getResource(resource);
        if (url != null) {
            if ("file".equals(url.getProtocol())) {
                try {
                    System.load(new File(url.toURI()).getAbsolutePath());
                    loaded = true;
                    return;
                } catch (java.net.URISyntaxException e) {
                    // Fall through to copying it.
                }
            }
            try (InputStream in = url.openStream()) {
                Path dir = Files.createTempDirectory("fusee-jni");
                Path lib = dir.resolve(name);
                Files.copy(in, lib, StandardCopyOption.REPLACE_EXISTING);
                lib.toFile().deleteOnExit();
                dir.toFile().deleteOnExit();
                System.load(lib.toAbsolutePath().toString());
                loaded = true;
                return;
            } catch (IOException e) {
                throw new UnsatisfiedLinkError("cannot extract " + resource + ": " + e);
            }
        }
        try {
            System.loadLibrary("fusee_jni");
            loaded = true;
        } catch (UnsatisfiedLinkError e) {
            UnsatisfiedLinkError u = new UnsatisfiedLinkError("no libfusee_jni for " + platform + " on the class path ("
                    + resource + ") or java.library.path; build it with bindings/java/native/build.sh, or set "
                    + "-Dfusee.library=/path/to/" + name);
            u.initCause(e);
            throw u;
        }
    }

    /** darwin-arm64, darwin-x86_64, linux-x86_64 or linux-aarch64 (uname -s / -m). */
    static String platform() {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        String arch = System.getProperty("os.arch", "").toLowerCase(Locale.ROOT);
        String o = os.contains("mac") || os.contains("darwin") ? "darwin" : os.contains("linux") ? "linux"
                : os.replaceAll("[^a-z0-9]", "");
        boolean arm = arch.equals("aarch64") || arch.equals("arm64");
        boolean x86 = arch.equals("amd64") || arch.equals("x86_64") || arch.equals("x64");
        String a = arm ? (o.equals("darwin") ? "arm64" : "aarch64") : x86 ? "x86_64" : arch;
        return o + "-" + a;
    }
}
