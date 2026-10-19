// S9: a fresh install is usable without a separate step. The leader prints a one-time password, and the
// administrator signs in with it and has to choose their own before anything else.
import { test, expect, signInThroughGui } from '../fleet';

test('the administrator signs in with the one-time password and chooses their own', async ({
  page,
  leader,
}) => {
  await page.goto(leader.url);
  await expect(page.getByRole('heading', { name: 'Sign in to smeltr' })).toBeVisible();

  await signInThroughGui(page, leader.username, leader.oneTimePassword);
  await expect(page.getByRole('heading', { name: 'Choose your own password' })).toBeVisible();

  const chosen = 'first-start-password';
  await page.getByLabel('New password', { exact: true }).fill(chosen);
  await page.getByLabel('Confirm new password').fill(chosen);
  await page.getByRole('button', { name: 'Change password' }).click();

  await expect(page.getByRole('status')).toHaveText(
    'password changed -- sign in with your new one',
  );

  await signInThroughGui(page, leader.username, leader.oneTimePassword);
  await expect(page.getByRole('alert')).toBeVisible();

  await signInThroughGui(page, leader.username, chosen);
  await expect(page.getByRole('navigation').first()).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Sign in to smeltr' })).toHaveCount(0);
  // W1, W8: a browser that remembers no branch opens on what the fleet runs.
  await expect(page.getByRole('button', { name: /^Branch: / })).toHaveAccessibleName(
    'Branch: main · read-only',
  );
});

test('two passwords that differ are refused before the leader is asked', async ({
  page,
  leader,
}) => {
  await page.goto(leader.url);
  await signInThroughGui(page, leader.username, leader.oneTimePassword);
  await page.getByLabel('New password', { exact: true }).fill('one-password-here');
  await page.getByLabel('Confirm new password').fill('another-password');
  await page.getByRole('button', { name: 'Change password' }).click();
  await expect(page.getByRole('alert')).toHaveText('those two passwords are not the same');
});
