/** A training video. One with no `youtube` id is not out yet: its course lists it as coming soon. */
export type Video = {
  title: string;
  /** The video's page is /learn/{course}/{slug}. */
  slug: string;
  youtube?: string;
  minutes?: number;
  poster?: string;
};

/** Videos watched in order. The course's page is /learn/{slug}. */
export type Course = {
  title: string;
  slug: string;
  /** The demo has what the course shows, so the course links to it. */
  demo?: true;
  videos: Video[];
};

/** The video the home page shows. */
export const walkthrough: Video = {
  title: 'Fixing a feed in smeltr',
  slug: 'fixing-a-feed',
  youtube: 't_6-JqXfFtM',
  minutes: 6,
  poster: '/videos/walkthrough-poster.jpg',
};

const allCourses: Course[] = [
  {
    title: 'Demo walkthrough',
    slug: 'demo-walkthrough',
    demo: true,
    videos: [walkthrough],
  },
  {
    title: 'Getting started',
    slug: 'getting-started',
    videos: [
      { title: 'Install smeltr', slug: 'install' },
      { title: 'Review and deploy a change', slug: 'review-and-deploy' },
    ],
  },
  {
    title: 'Data engineering with the smeltr agent',
    slug: 'agent',
    videos: [
      {
        title: 'Set Up Your AI Data Engineer in smeltr',
        slug: 'setup',
        youtube: 'DiXMyUMlLWw',
        minutes: 3,
        poster: '/videos/agent-course-1-poster.jpg',
      },
      {
        title: 'AI Agent Builds a FortiGate to Elasticsearch Pipeline',
        slug: 'build-a-pipeline',
        youtube: 'QqPgbVsE2I0',
        minutes: 3,
        poster: '/videos/agent-course-2-poster.jpg',
      },
      {
        title: 'Review and Deploy What Your AI Agent Built',
        slug: 'review-and-deploy',
        youtube: 'P8AqcVGqBxo',
        minutes: 5,
        poster: '/videos/agent-course-3-poster.jpg',
      },
      {
        title: 'Ask Your AI Agent About Your Fleet',
        slug: 'ask-about-your-fleet',
        youtube: '48eIRNY_K50',
        minutes: 2,
        poster: '/videos/agent-course-4-poster.jpg',
      },
      {
        title: 'Have Your AI Agent Fix a Pipeline Problem',
        slug: 'fix-a-problem',
        youtube: 'yXhaDPrdRc4',
        minutes: 3,
        poster: '/videos/agent-course-5-poster.jpg',
      },
    ],
  },
];

export const released = (course: Course) => course.videos.filter((v) => v.youtube);

/** The courses the site shows: those with a video out. */
export const courses = allCourses.filter((c) => released(c).length);

/** How many videos the course has and how long the released ones run, as its cards say it. */
export function courseLength(course: Course): string {
  const out = released(course);
  const soon = course.videos.length - out.length;
  const minutes = out.reduce((sum, v) => sum + (v.minutes ?? 0), 0);
  const videos = out.length === 1 ? '1 video' : `${out.length} videos`;
  return `${videos}${soon ? `, ${soon} coming soon` : ''} · ${minutes} min`;
}
