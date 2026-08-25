import { defineConfig } from 'tsup';

export default defineConfig({
  entry: ['src/index.ts', 'src/testing.ts', 'src/node.ts'],
  format: ['esm', 'cjs'],
  dts: true,
  clean: true,
  sourcemap: true,
  external: ['@stepd/protocol', '@stepd/sdk-core'],
});
