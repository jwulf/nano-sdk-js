import { defineConfig } from "tsup";

export default defineConfig({
  entry: ["src/index.ts"],
  format: ["esm", "cjs"],
  dts: true,
  clean: true,
  sourcemap: true,
  target: "es2022",
  // The upstream SDK and ws stay external — we re-export/wrap them.
  external: ["@camunda8/orchestration-cluster-api", "ws"],
});
