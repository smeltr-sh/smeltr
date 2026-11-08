/** A topic on the Learn page. */
export type Topic = 'start' | 'build' | 'operate';

export const topicNames: Record<Topic, string> = {
  start: 'Get started',
  build: 'Build & test',
  operate: 'Deploy & operate',
};

/** A training video. One with no `youtube` id is planned: its card says so and links the guide that covers it today. */
export type Video = {
  topic: Topic;
  title: string;
  /** The video's page is /learn/{slug}. */
  slug: string;
  /** Linked from the card while the video is planned. */
  guide: string;
  youtube?: string;
  minutes?: number;
  poster?: string;
};

/** The video the home page shows. */
export const walkthrough: Video = {
  topic: 'build',
  title: 'Fixing a feed in smeltr',
  slug: 'fixing-a-feed',
  guide: '/docs/first-pipeline',
  youtube: 't_6-JqXfFtM',
  minutes: 6,
  poster: '/videos/walkthrough-poster.jpg',
};

export const videos: Video[] = [
  { topic: 'start', title: 'Install smeltr', slug: 'install', guide: '/docs/getting-started' },
  walkthrough,
  {
    topic: 'operate',
    title: 'Review and deploy a change',
    slug: 'review-and-deploy',
    guide: '/docs/review',
  },
];
