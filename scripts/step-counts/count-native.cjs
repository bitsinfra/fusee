// Counts calls into Fusee's Node addon: wraps its exported functions and its
// classes' prototype methods as the addon loads. __fuseeNativeCalls() reads it.
const counts = Object.create(null);
let total = 0;
const bump = (k) => { counts[k] = (counts[k] || 0) + 1; total++; };
const orig = process.dlopen;
process.dlopen = function (module, filename, ...rest) {
  const r = orig.call(this, module, filename, ...rest);
  if (/fusee\.[^/]*\.node$/.test(filename)) instrument(module.exports);
  return r;
};
function instrument(m) {
  for (const k of Object.keys(m)) {
    const v = m[k];
    if (typeof v !== "function") continue;
    if (/^[A-Z]/.test(k)) {
      for (const name of Object.getOwnPropertyNames(v.prototype)) {
        if (name === "constructor") continue;
        const d = Object.getOwnPropertyDescriptor(v.prototype, name);
        if (!d || typeof d.value !== "function") continue;
        const f = d.value;
        Object.defineProperty(v.prototype, name, { ...d, value: function (...a) { bump(`${k}.${name}`); return f.apply(this, a); } });
      }
    } else {
      m[k] = function (...a) { bump(k); return v.apply(this, a); };
    }
  }
}
globalThis.__fuseeNativeCalls = () => ({ total, counts: { ...counts } });
