import json from "@rollup/plugin-json";
import typescript from "@rollup/plugin-typescript";
import { nodeResolve } from "@rollup/plugin-node-resolve";
import commonjs from "@rollup/plugin-commonjs";
import esbuild from "rollup-plugin-esbuild";

export default {
  input: "src/index.ts", // Điểm vào từ mã TypeScript. -- phương án build trong tham số lệnh
  external: [], // Không có external deps
  output: [
    {
      file: "lib/index.js", // CommonJS use: import ... from '...';
      format: "cjs",
      sourcemap: true, // Tùy chọn: Thêm source map để debug
      exports: "auto",
    },
    {
      file: "lib/index.mjs", // ES Module: use = require('..');
      format: "esm",
      sourcemap: true,
    },
    {
      file: "lib/index.umd.js", // UMD cho browser và hybrid
      format: "umd",
      name: "DQCAIMongoDB", // Tên global variable cho browser
      sourcemap: true,
    },
  ],
  plugins: [
    json(), // Thêm plugin này
    nodeResolve(), // Giải quyết các phụ thuộc node_modules
    commonjs(), // Chuyển đổi CommonJS sang ESM nếu cần
    typescript({
      tsconfig: "./tsconfig.json",
      declaration: true,
      declarationDir: "lib",
      declarationMap: true, // Tạo .d.ts.map để debug
      rootDir: "src",
    }), // Biên dịch TypeScript
    esbuild({
      minify: true,
      target: "es2017", // Target ES version phù hợp: ES2017+ cho modern browsers // RN 0.60+
    }),
  ],
  external: ['mongodb'] // MongoDB nên là external dependency
};
