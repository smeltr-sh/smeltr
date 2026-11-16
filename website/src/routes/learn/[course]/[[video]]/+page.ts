import { error } from '@sveltejs/kit';
import { transcript } from '#lib/transcripts.ts';
import { courses, released } from '#lib/videos.ts';

export const entries = () =>
  courses.flatMap((c) => [
    { course: c.slug, video: '' },
    ...released(c).map((v) => ({ course: c.slug, video: v.slug })),
  ]);

/** Without a video in the path, the course page shows its overview, or else plays its first released video. */
export async function load({ params }: { params: { course: string; video?: string } }) {
  const course = courses.find((c) => c.slug === params.course);
  const video = params.video
    ? course && released(course).find((v) => v.slug === params.video)
    : course && released(course)[0];
  if (!course || !video) error(404, 'No such video');
  // A course with an overview shows it at its own page, with no video playing there.
  const overview = !params.video && course.about;
  return { course, video, transcript: overview ? [] : await transcript(course.slug, video.slug) };
}
