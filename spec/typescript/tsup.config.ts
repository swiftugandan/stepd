import { defineConfig } from 'tsup';

export default defineConfig({
  entry: ['src/index.ts'],
  format: ['esm', 'cjs'],
  dts: true,
  clean: true,
  sourcemap: true,
  // Not bundled: `@noble/hashes` is a real dependency, and inlining it would
  // make two copies in any app that also depends on it directly.
  external: ['@noble/hashes'],
});
