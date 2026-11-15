import { error } from '@sveltejs/kit';
import { courses, selfTested } from '#lib/videos.ts';

export const entries = () => selfTested.map((c) => ({ course: c.slug }));

/** `listed` is whether the course's own page is out, so the self-test can link to it. */
export function load({ params }: { params: { course: string } }) {
  const course = selfTested.find((c) => c.slug === params.course);
  if (!course?.selfTest) error(404, 'No such self-test');
  return { course, test: course.selfTest, listed: courses.includes(course) };
}
