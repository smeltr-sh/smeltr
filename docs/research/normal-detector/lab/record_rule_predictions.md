# Record rule: predictions, written before the replay (2026-11-11 00:20 UTC)

Rule. Remember every silence of the past year that ended without a warning. Warn when the current silence is longer
than all of them. While less than a year has been watched, add what the rest of the year would add to the longest:
m * ln(year / time watched), m = (sum of s^2) / (2 * sum of s) over the remembered silences (for silences that end at
a steady chance per minute, m is their mean; a burst's short silences barely move it). Judging starts when today's
learning period ends.

Why. Among the long silences of a year plus the next one, each is equally likely to be the longest, so a healthy source
makes a silence longer than all of the past year's about once a year at most, whatever its shape, burst or not. The
false-warning period is the one choice. For silences that end at a steady chance, the longest of n grows by about m
each time n grows e-fold, which gives the learning term.

Predictions:
1. Once a year is watched, every source false-warns at most about once a year (heartbeat: almost never, its silences
   are equal). In the first year about the same if the learning term holds; more on the rare bursty feed, whose m
   rests on two or three silences.
2. Rare feed, about every 14 days at random: a stop is noticed about 40 days after a random moment (the year's longest
   gap, about 51 days, less the silence already under way). Every 10 to 18 days: about 9 days. Today's gap: judged
   rarely and false-warns several times a year when it is.
3. Bursts change nothing against single rows at the same timing.
4. One row an hour at random: about 9 hours, as today. One a day: about 6 days, as today. Hourly heartbeat: within an
   hour of the missed row once a year is watched, about 2 to 3 hours in the first months.
5. Ten logs (all under a year, so always learning): false warnings within about half to double today's gap.

# What the first replay showed (record_c1.json), and the corrected rule (2026-11-11 00:40 UTC, before its replay)

Prediction 1 failed: after the first year the record rule false-warned 14 times a year on the rare bursty feed, 3.7 on
one row an hour. Cause: a silence that warned is never remembered, so with no margin the longest remembered silence can
only fall as old ones age out, and every silence above it warns again. The margin is therefore not only a false-warning
knob: it is the room in which a source can show a longer silence than before and have it learned. It must be over 1.

Today's gap measured as the theory says (false warnings a year = gaps a year x 1.33 / n^1.5, n = gaps in memory):
one row an hour 0.67 predicted, 0.67 to 0.72 measured; one row a day 3.3 predicted, 3.6 to 3.8 measured.

Corrected rule: today's gap, remembering a year instead of 28 days. Warn when the current silence is 1.5 times the
longest silence of the past year that ended without a warning. In the first year the longest is taken as the longest so
far plus m * ln(year / watched). No ten-silence gate.

Predictions:
1. After the first year: every 14 days at random 0.26 false a year; one row a day 0.07; one row an hour, heartbeat,
   daytime only, every 10 to 18 days: about 0. First year: similar, somewhat more on the rare feed.
2. Stops: every 14 days at random about 65 days after a random moment; every 10 to 18 days about 20; one row an hour
   about 13 hours (today 9); one row a day about 9 days (today 4.5); heartbeat about 35 minutes after the missed row.
3. Bursts change nothing.
4. Ten logs: fewer false warnings than today, slower stops.
