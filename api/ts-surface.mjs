// The public declarations of a TypeScript entry point, as one file: follows
// its `export { … } from "./module.js"` lines (through other re-exporting
// modules) into the modules' declaration files, prints each exported
// declaration as tsc wrote it, then the module declarations those refer to
// (a base class, an option's type), so the report shows the whole surface.
//
//   node api/ts-surface.mjs bindings/node/dist/index.d.ts
import { readFileSync, readdirSync } from "node:fs";
import { createRequire } from "node:module";
import { basename, dirname, join, resolve } from "node:path";

const entry = resolve(process.argv[2]);
const dir = dirname(entry);
const require = createRequire(join(dir, "..", "package.json"));
const ts = require("typescript");

const files = new Map();
const parse = (file) => {
  if (!files.has(file)) files.set(file, ts.createSourceFile(file, readFileSync(file, "utf8"), ts.ScriptTarget.ES2022, true));
  return files.get(file);
};
const dts = (spec) => join(dir, spec.replace(/\.js$/, ".d.ts"));
const namesOf = (st) => {
  if (ts.isVariableStatement(st)) return st.declarationList.declarations.map((d) => d.name.getText());
  return st.name && ts.isIdentifier(st.name) ? [st.name.text] : [];
};

/** Where `name`, exported by `file`, is declared: [file, statement]. */
function resolveExport(file, name, seen = new Set()) {
  if (seen.has(file + "#" + name)) return null;
  seen.add(file + "#" + name);
  const sf = parse(file);
  for (const st of sf.statements) {
    if (namesOf(st).includes(name)) return [file, st];
  }
  for (const st of sf.statements) {
    if (ts.isExportDeclaration(st) && st.moduleSpecifier && st.exportClause && ts.isNamedExports(st.exportClause)) {
      const el = st.exportClause.elements.find((e) => e.name.text === name);
      if (el) return resolveExport(dts(st.moduleSpecifier.text), (el.propertyName ?? el.name).text, seen);
    }
  }
  return null;
}

// The entry point's exports, in order.
const root = parse(entry);
const picked = []; // [file, statement]
const have = new Set();
const add = (r) => {
  if (!r) return;
  const key = r[0] + ":" + r[1].pos;
  if (have.has(key)) return;
  have.add(key);
  picked.push(r);
};
for (const st of root.statements) {
  if (ts.isExportDeclaration(st) && st.moduleSpecifier && st.exportClause && ts.isNamedExports(st.exportClause)) {
    for (const el of st.exportClause.elements) add(resolveExport(entry, el.name.text));
  } else if (!ts.isImportDeclaration(st) && !ts.isExportDeclaration(st) && namesOf(st).length) {
    add([entry, st]);
  }
}
const publicCount = picked.length;

// Declarations the public ones refer to, in any module, to a fixpoint.
const declared = new Map(); // name -> [file, statement]
for (const f of [...new Set([entry, ...[...files.keys()]])]) {
  for (const st of parse(f).statements) for (const n of namesOf(st)) if (!declared.has(n)) declared.set(n, [f, st]);
}
for (const f of readdirModules()) {
  for (const st of parse(f).statements) for (const n of namesOf(st)) if (!declared.has(n)) declared.set(n, [f, st]);
}
for (let i = 0; i < picked.length; i++) {
  const [, st] = picked[i];
  const visit = (node) => {
    if (ts.isIdentifier(node) && declared.has(node.text) && !isMemberName(node)) add(declared.get(node.text));
    ts.forEachChild(node, visit);
  };
  visit(st);
}

/** A member's, parameter's or type parameter's own name, not a reference. */
function isMemberName(node) {
  const p = node.parent;
  if (!p) return false;
  if ((ts.isPropertyAccessExpression(p) || ts.isQualifiedName(p)) && (p.name ?? p.right) === node) return true;
  return (
    (ts.isPropertySignature(p) || ts.isMethodSignature(p) || ts.isPropertyDeclaration(p) || ts.isMethodDeclaration(p) ||
      ts.isParameter(p) || ts.isGetAccessor(p) || ts.isSetAccessor(p) || ts.isEnumMember(p) || ts.isTypeParameterDeclaration(p) ||
      ts.isPropertyAssignment(p) || ts.isBindingElement(p)) &&
    p.name === node
  );
}

function readdirModules() {
  // Every module's declarations (not the other entry points').
  const entries = new Set(["index.d.ts", "client.d.ts", "testing.d.ts", "ai.d.ts", "rich.d.ts"]);
  return readdirSync(dir)
    .filter((f) => f.endsWith(".d.ts") && !entries.has(f))
    .map((f) => join(dir, f));
}

let out = "";
let last = "";
picked.forEach(([file, st], i) => {
  const head = `// ${basename(file).replace(/\.d\.ts$/, "")}${i >= publicCount ? " (referenced)" : ""}`;
  if (head !== last) {
    out += (out ? "\n" : "") + head + "\n";
    last = head;
  }
  out += st.getFullText(parse(file)).replace(/^\n+/, "").replace(/^export /, i >= publicCount ? "" : "export ") + "\n";
});
process.stdout.write(out.replace(/\/\/# sourceMappingURL=.*\n?/g, "").replace(/\n{3,}/g, "\n\n"));
