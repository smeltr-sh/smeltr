import { describe, expect, it } from 'vitest';
import { editedBy, suggestBranchName } from './branchNames';

describe('editedBy', () => {
  it('names what a write changes, and the worker group when it is not shared', () => {
    expect(editedBy('/api/areas/shared/routes?branch=main')).toBe('routes');
    expect(editedBy('/api/areas/network/sources')).toBe('network-sources');
    expect(editedBy('/api/areas/shared/pipelines/fortigate')).toBe('pipeline-fortigate');
    expect(editedBy('/api/areas/shared/files/pipelines/cisco-asa.yaml')).toBe('cisco-asa');
    expect(editedBy('/api/worker-groups/east')).toBe('worker-group-east');
    expect(editedBy('/api/secrets/db%20password')).toBe('secret-db password');
  });
});

describe('suggestBranchName', () => {
  it('is who and what, in a shape git takes as it is', () => {
    expect(suggestBranchName('Dana', 'routes', [])).toBe('dana-routes');
    expect(suggestBranchName('Dana Lee', 'secret-db password', [])).toBe(
      'dana-lee-secret-db-password',
    );
    expect(suggestBranchName('', '', [])).toBe('edit');
  });

  it('takes a number when the name is taken', () => {
    expect(suggestBranchName('dana', 'routes', ['dana-routes'])).toBe('dana-routes-2');
    expect(suggestBranchName('dana', 'routes', ['dana-routes', 'dana-routes-2'])).toBe(
      'dana-routes-3',
    );
  });
});
