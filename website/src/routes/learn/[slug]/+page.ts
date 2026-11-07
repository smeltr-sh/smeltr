import { error } from '@sveltejs/kit';
import { videos } from '#lib/videos.ts';

const ready = videos.filter((v) => v.youtube);

export const entries = () => ready.map((v) => ({ slug: v.slug }));

export function load({ params }: { params: { slug: string } }) {
  const video = ready.find((v) => v.slug === params.slug);
  if (!video) error(404, 'No such video');
  return { video };
}
