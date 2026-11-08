import { error } from '@sveltejs/kit';
import { courses, released } from '#lib/videos.ts';

export const entries = () =>
  courses.flatMap((c) => [
    { course: c.slug, video: '' },
    ...released(c).map((v) => ({ course: c.slug, video: v.slug })),
  ]);

/** Without a video in the path, the course page plays its first released video. */
export function load({ params }: { params: { course: string; video?: string } }) {
  const course = courses.find((c) => c.slug === params.course);
  const video = params.video
    ? course && released(course).find((v) => v.slug === params.video)
    : course && released(course)[0];
  if (!course || !video) error(404, 'No such video');
  return { course, video };
}
