import { defineConfig } from "vite";

export default defineConfig({
  root: "browser",
  base: "./",
  assetsInclude: ["**/*.data"],
  build: {
    outDir: "dist",
  },
});
