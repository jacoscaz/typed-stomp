#!/usr/bin/env node
/// <reference types="node" />
import process from 'node:process';
import { run } from './cli.ts';

const controller = new AbortController();
process.once('SIGINT', () => controller.abort());
process.once('SIGTERM', () => controller.abort());

process.exitCode = await run(process.argv.slice(2), {
  stdout: process.stdout,
  stderr: process.stderr,
  stdin: process.stdin,
  signal: controller.signal,
  env: process.env,
});
