// Build the website into site/:
//   site/index.html   the visualizer, with app.js and style.css beside it
//   site/quorum.html  the same app as one self-contained file
import { build } from "esbuild";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";

const out = "site";
mkdirSync(out, { recursive: true });
const result = await build({
  entryPoints: ["web/app.ts"],
  bundle: true,
  minify: true,
  format: "iife",
  target: "es2020",
  write: false,
});
const js = result.outputFiles[0].text;
const css = readFileSync("web/style.css", "utf8");
const html = readFileSync("web/index.html", "utf8");

writeFileSync(`${out}/app.js`, js);
writeFileSync(`${out}/style.css`, css);
writeFileSync(`${out}/index.html`, html
  .replace("<!--STYLE-->", '<link rel="stylesheet" href="style.css">')
  .replace("<!--SCRIPT-->", '<script src="app.js"></script>'));
writeFileSync(`${out}/quorum.html`, html
  .replace("<!--STYLE-->", () => `<style>\n${css}\n</style>`)
  .replace("<!--SCRIPT-->", () => `<script>\n${js.replace(/<\/script/g, "<\\/script")}\n</script>`));
console.log(`built ${out}/ (app.js ${(js.length / 1024).toFixed(0)} KB)`);
