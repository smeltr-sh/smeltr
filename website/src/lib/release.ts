// Where releases are published. The repository's owner is named once, here, so moving it is a one-line change.
const repository = 'smeltr-sh/smeltr';

/** The source code. */
export const source = `https://github.com/${repository}`;

/** The container image; `latest` moves to each newest release. */
export const image = `ghcr.io/${repository}`;

/** A file attached to the newest release. Release file names carry no version, so this link never changes. */
export function latestFile(name: string): string {
  return `https://github.com/${repository}/releases/latest/download/${name}`;
}

/** The license, as the repository carries it. */
export const license = `https://github.com/${repository}/blob/main/LICENSE`;
