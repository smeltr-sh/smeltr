import { error } from '@sveltejs/kit';
import { releases } from '#lib/releases.ts';

export const entries = () => releases.map((r) => ({ version: r.version }));

export function load({ params }: { params: { version: string } }) {
  const release = releases.find((r) => r.version === params.version);
  if (!release) error(404, 'No such release');
  return { release };
}
