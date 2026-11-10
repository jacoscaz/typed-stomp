import { execFile } from 'node:child_process';
import { promisify } from 'node:util';

const exec = promisify(execFile);

/** Runs the docker CLI and returns its standard output. */
export async function docker(args: readonly string[], timeout = 60_000): Promise<string> {
  const { stdout } = await exec('docker', [...args], { timeout, maxBuffer: 64 * 1024 * 1024 });
  return stdout.trim();
}

export async function dockerAvailable(): Promise<boolean> {
  try {
    await docker(['info', '--format', '{{.ServerVersion}}'], 10_000);
    return true;
  } catch {
    return false;
  }
}

export async function buildImage(tag: string, context: string): Promise<void> {
  await docker(['build', '--quiet', '--tag', tag, context], 15 * 60_000);
}

export interface Container {
  readonly id: string;
  /** Host port mapped to the given container port. */
  port(containerPort: number): Promise<number>;
  logs(): Promise<string>;
  remove(): Promise<void>;
}

/**
 * Starts a container in the background, publishing ports on 127.0.0.1,
 * optionally overriding the image's command.
 */
export async function runDetached(
  image: string,
  ports: readonly number[],
  command: readonly string[] = [],
): Promise<Container> {
  const publish = ports.flatMap((port) => ['--publish', `127.0.0.1::${port}`]);
  const id = await docker(['run', '--detach', ...publish, image, ...command]);
  return {
    id,
    async port(containerPort) {
      const mapping = await docker(['port', id, `${containerPort}/tcp`]);
      const match = /:(\d+)\s*$/m.exec(mapping.split('\n')[0] ?? '');
      if (match === null) {
        throw new Error(`Cannot parse port mapping: ${mapping}`);
      }
      return Number(match[1]);
    },
    async logs() {
      const { stdout, stderr } = await exec('docker', ['logs', id], { maxBuffer: 64 * 1024 * 1024 });
      return stdout + stderr;
    },
    async remove() {
      await docker(['rm', '--force', id]);
    },
  };
}

/**
 * Runs a container to completion and returns its exit code and output.
 * `host.docker.internal` resolves to the host inside the container.
 */
export async function runToCompletion(
  image: string,
  env: Readonly<Record<string, string>>,
  timeout = 120_000,
): Promise<{ code: number; output: string }> {
  const envArgs = Object.entries(env).flatMap(([key, value]) => ['--env', `${key}=${value}`]);
  const args = ['run', '--rm', '--add-host=host.docker.internal:host-gateway', ...envArgs, image];
  try {
    const { stdout, stderr } = await exec('docker', args, { timeout, maxBuffer: 64 * 1024 * 1024 });
    return { code: 0, output: stdout + stderr };
  } catch (err) {
    const error = err as { code?: number; stdout?: string; stderr?: string };
    return { code: typeof error.code === 'number' ? error.code : -1, output: `${error.stdout ?? ''}${error.stderr ?? ''}` };
  }
}

/** Retries `attempt` until it resolves, or rethrows its last error after `timeout` ms. */
export async function retry<T>(attempt: () => Promise<T>, timeout = 120_000, interval = 500): Promise<T> {
  const deadline = Date.now() + timeout;
  for (;;) {
    try {
      return await attempt();
    } catch (err) {
      if (Date.now() > deadline) {
        throw err;
      }
      await new Promise((resolve) => setTimeout(resolve, interval));
    }
  }
}

/** Polls an HTTP URL until the server answers, whatever the status. */
export async function waitForHttp(url: string, timeout = 120_000): Promise<void> {
  const deadline = Date.now() + timeout;
  for (;;) {
    try {
      await fetch(url);
      return;
    } catch (err) {
      if (Date.now() > deadline) {
        throw new Error(`${url} did not become available`, { cause: err });
      }
      await new Promise((resolve) => setTimeout(resolve, 250));
    }
  }
}
