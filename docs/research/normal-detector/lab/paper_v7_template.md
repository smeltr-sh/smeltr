# Detecting a drop in a count against its own history: one judge at five widths, and what it catches

*2026-11-17 · John Pavlik · Claude (Anthropic)*

## Abstract

A source emits a count every five minutes, and an operator needs three things from a detector of drops in it, in order: to know when the source has stopped, to trust the alarm, and to hear of a real degradation as soon as the first two allow. We recommend one detector for all three. It is one judge: a statistic of the source's recent past is held against the empirical quantile of the same statistic over the source's own history, read against the typical level of its time of day and kind of day, a margin is set beyond the quantile, and the alarm is raised after a wait. The judge is applied at five widths: four count the rows of the last 5 minutes, 30 minutes, 6 hours and 1 day, and the fifth counts the time since the last row. A width judges only where the source gives it enough data for a quantile, so a dense source is judged in rows and a sparse one in time, and every edge is a quantile of the source's own past, with no distribution assumed. On 35 sources (14 count series, 10 logs, 7 modeled shapes and 4 sparse sources) the detector catches every injected stop, within 20 minutes on a source with rows in every bucket at the Balanced preset. Against the baseline, false alarms fall from {{base_false_real}} to {{false_bal_real}} per source-month on the real count series, and a drop to 30% is caught within the day {{by1440_03_bal_real}} of the time.

## 1. Introduction

Operational systems watch counts: rows read from a source, requests served, lines written to a log, readings received from a fleet of sensors. The baseline, the rule this work replaces, watched them the obvious way: it compared the last 30 minutes with the 91 windows at the same time of day over the past week and alarmed when the current window fell below their 99% band. On real count series it raised about {{base_false_real}} false alarms per source-month, {{base_per_day}} a day on a healthy source. An alarm that is wrong {{base_per_day}} times a day is an alarm nobody reads, and a detector nobody reads misses the drop that mattered. That is the problem this paper starts from, and the reason it treats false alarms as the first cost and not an afterthought.

What does an operator need from a drop detector? Three things, in order. First, to know when a source has stopped, on every source, whatever its rate: a source that has gone silent is the one failure that is always a failure. Second, to be able to trust the alarm: a healthy source must almost never alarm, or the first need is not met either, because the stop's alarm is one among the day's false ones. Third, to hear about a real degradation, a source that has fallen to a tenth or a third of its usual rate, as soon as the first two needs allow. The order matters because the three pull against each other: every loosening that catches a shallower drop sooner also lets more healthy days through. This paper takes the order as given and reports where each need is met, where it is paid for, and where it is given up.

Why did the baseline fail, and what fixes it? Its 91 windows overlap in time and share slowly drifting noise, so they hold far fewer independent observations than their number suggests, and a reading at the band's edge flickers in and out, each flicker a new alarm. The repair rests on one distribution-free fact (Appendix A): among n exchangeable windows and one more, the new one is the lowest at most 1/(n + 1) of the time, and exactly that when no two tie. The fact lets the source's own past draw its band with no distribution assumed, which is what the heavy-tailed counts of real systems need. But the fact alone does not reach one alarm a month. A width judged every five minutes is read 8,640 times a month. An edge at the lowest of a week's two thousand windows is undercut by a healthy source about four of those times by chance, and the design's edge, which sits near the third-lowest, about thirteen. A quantile by itself therefore stops an order of magnitude short of the alarm rate the second need asks for. So the detector pulls three levers the fact leaves open (Section 2). It pools every window of the week, after dividing each by the typical level of its time of day and kind of day, so that two thousand windows stand behind the edge instead of 91. It widens the edge by a margin, so a reading must fall further than any past one did, not merely below the lowest. And it waits for k readings in a row, four at Balanced, so a flicker at the edge pays nothing. Each lever is measured on its own in Section 6.1, and together they take the real series from {{base_false_real}} false alarms a month to {{false_bal_real}}.

One judge, then, applied to what? A stop empties a 5-minute window in 5 minutes and a day's window in a day, and a shallow drop that never empties a 5-minute window pulls a day's window far below its usual sum, so no single window sees every drop. The judge is therefore applied at several widths of window, 5 minutes, 30 minutes, 6 hours and 1 day, each with its own quantile over the same history, and a width is judged only where the source's typical window at that width holds ten rows, because a quantile of ratios among a handful of rows means nothing. That one gate is what makes the detector follow the source's density: a dense source is judged at every width and its stop is caught at the k-th empty 5-minute reading, 20 minutes at Balanced; a source with four rows an hour is judged at 6 hours and 1 day; a source with a row an hour at 1 day alone. The stop is caught as fast as the source's own rows allow and no faster, and no preset changes that (Section 5.1).

Below a row an hour the count widths run out, and the judge does not. A log that writes a line only when something goes wrong, a service whose rows come in bursts with days of silence between, a source with one row a day: on these no window up to a day usually holds ten rows, or the history holds empty windows at every time of day and the lower edge is zero, so a count of rows per unit of time can never fall below normal. But every such source has a normal in the other coordinate, time per row: its quiet spells have lengths, and an operator asking whether the source has been silent too long is asking whether the current spell is longer than its spells ever are. So the fifth width is the gap, the time since the last row, judged by the same judge against the source's own completed spells of the past year: the source is stopped when its current silence has outlasted every one of them by half again (Section 2.8). The year is chosen, not tuned. A healthy source outlasts every spell of its history at most about once per history length, whatever the shape of its spells and however they bunch, so the length of the history is the false-alarm period; a source that fires in bursts every two weeks has two spells in 28 days and twenty-six in a year. A dense source has no spells and is judged in rows; a source with one row a day fills no count window and is judged in time; a thin source is judged in both, and a stop is reported by whichever width fires first. Scored on the same 35 sources, from web servers at hundreds of lines a minute down to a source with one row a day, the detector catches every injected stop, {{either_day_real}} within the day on the real count series and on seven of the ten logs (Section 4.8), and the gap raises no false alarm on any count series or sparse source. On a modeled source that fires in bursts every two weeks at random it raises {{rare_false_year}} false alarms a year, where 28 days of spells raised {{rare_false_28}} (Section 4.7).

The same detector then watches the senders behind a source. A source that gathers the rows of a hundred sensors loses one of them without its total moving by more than the noise, so each sender's own count is judged by the same band, at the Quiet preset, and a sender is any value of a field with rows in every hour of seven days in a row (Sections 2.6 and 4.5).

What is given up is named. At Balanced a drop to 30% is caught within the hour on {{by60_03_bal_real}} of the real series' injected drops and a halving on {{by60_05_bal_real}}; by the end of the day the long widths catch {{by1440_03_bal_real}} and {{by1440_05_bal_real}}. Sections 6.1 and 6.2 show that no setting of the band buys the halving within the hour without the baseline's alarm rate, and the rest of Section 6 that none of the alternatives tried does either. The trade is the operator's to make, with the margin and the wait, and the paper gives the frontier they trace.

The headline, on the 14 real count series and the 21 modeled series, 7 shapes at 3 seeds, is

{{HEADLINE}}

Three things in that table must be read apart, because they behave differently.

*Full stops are caught at every preset*, because a stop is the one drop every width sees: a count width reads zero, and zero is below any positive lower edge, and the gap grows past any record. Section 4.8 scores the five widths together on every source: every injected stop is caught, {{either_day_real}} within the day on the real count series, {{either_day_mod}} on the modeled series, and within the day on seven of the ten logs. The only question is how soon, and the answer is set by the source, not by the margin or the wait: the narrowest count width whose windows usually hold rows catches the stop at its k-th empty reading, at Balanced 20 minutes on a source with rows in every 5-minute bucket and 45 minutes where the 30-minute width is the narrowest judged; below that the gap takes over, and a source with a dozen rows an hour is confirmed stopped in about {{e:sparse:12 rows an hour:median_either}}, one with four rows an hour in about {{e:sparse:4 rows an hour:median_either}}, one with a row an hour in about {{e:sparse:1 row an hour:median_either}} with the count widths beside it and {{qh:1 row an hour:1}} by the gap alone, and one that fires in bursts every two weeks in about {{rare_stop_year}} (Sections 4.4, 4.7 and 4.8). {{stops_day_note}} A source whose rows come in bursts with silences between is the gap's alone, which asks only whether the current quiet spell has outlasted every normal one of the year by a margin (Section 4.7).

*Degradations trade against false alarms.* A drop that leaves rows in the window is caught only when it pulls the window under the band's lower edge, and where that edge sits is what the margin and the wait buy. Section 5 is about that trade: which series pay it, why, and what each loosening buys back. The baseline's {{base_p30_real}} of drops to 30% within the hour rests on {{base_per_day}} false alarms a day per source, and Section 5.3 argues that at that rate its detection is on paper only.

*Time is the third axis.* The 6-hour and 1-day widths catch within their own horizon what no shorter width sees in an hour: a halving does not empty a day's window in an hour, but by the end of the day it has pulled it 50% under, and {{by1440_05_bal_real}} of halvings on real series are caught by then.

The margin and the wait are the two levers an operator sets; the four presets are points on the frontier they trace (Sections 2.7 and 5.2). Balanced is every source's preset until an operator picks another. Fast catches a stop at the first empty reading, 5 minutes on a dense source, for {{fast_ratio}} times Balanced's false alarms on the real series and about half its halvings within the day; Sensitive catches the most degradations, for {{false_sens_real}} false alarms a month; and Quiet, the senders' preset, nearly ends false alarms for {{quiet_cost}} points of drops to 30% within the hour. Section 6 gives the alternatives tried and the reasons they lost. The distribution-free fact that bounds the band's false-alarm rate, and the three levers it leaves, are in Appendix A. The paper follows one implementation, named in Section 8; where a choice comes from it rather than from the problem, such as how often the leader learns a source's normal again, we say so.

## 2. The detector

The detector is one judge applied at five widths, and Figure 1 says what each width is: the recipe down the side, the machinery in the cells, a cell that spans the four count widths shared and the gap's column its own. What the cells cannot carry is why. A count is judged as a ratio to the typical level of its time of day and kind of day because the bound of Appendix A asks for exchangeable windows, and a 3 a.m. window and a 3 p.m. window, or a Sunday and a Monday where the week has a shape, are exchangeable only as ratios. The long widths take 28 days of history because a week holds seven day-long windows that do not overlap, too few for a quantile. The gap pools its spells with no typical level because a record kept per hour of the day left each hour a handful of spells to beat and raised 5 to 17 false alarms per sender-month on a source of a hundred thin senders (Section 6.8). It takes the maximum of a year of spells because a healthy source outlasts every spell of its history at most about once per history length, so a year of history is a false-alarm period of a year, chosen rather than tuned (Appendix A, Corollary 3). It waits for one reading because a silence only grows, so a second reading says nothing the first did not, where a count at the band's edge flickers and is asked k times, four at Balanced. Sections 2.1 to 2.8 define each row.

<?claude block diagram?>

```text
judge(statistic, history, quantile, margin, wait, tail, unlearn):
    at every bucket t where this width is judged:
        edge      = quantile(history(t)), in the statistic's units,
                    moved by margin toward the tail
        beyond(t) = statistic(t) lies past edge on the tail side
        if beyond holds at t and at the wait - 1 readings before it:
            alarm, until a reading is not beyond
            unlearn what the alarm covers

for each count width w in {5 min, 30 min, 6 h, 1 day}:
    judge(statistic = S_w(t),                           # rows in the window
          history   = (S_w + 1) / (M_w + 1) of the learned windows, 7 days (short) or 28 (long),
                      M_w the typical window of the time of day and kind of day,
          quantile  = 0.05%, back in rows as (M_w(t) + 1) q - 1,
          margin, wait = the preset's (30% and 4 at Balanced), tail = lower,
          unlearn   = the buckets of every window alarmed or waited on,
                      from the one set L the four count widths share)

the gap:
    judge(statistic = g(t),                             # empty buckets since the last row
          history   = lengths of the completed spells of the past year,
          quantile  = the maximum, plus the first year's allowance (Section 2.8),
          margin = 50%, wait = 1, tail = upper,
          unlearn   = the spell alarmed on)

below normal at t iff some width is alarmed at t
```

Time is discrete in buckets of Δ = 5 minutes; D = 288 buckets make a day. A source is a count series x_t ≥ 0, t ∈ ℤ. A window of width w buckets ending at t has sum

```latex
S_w(t) = \sum_{i=0}^{w-1} x_{t-i}.
```

A reading is one judgment of the source at bucket t. An alarm is raised at t when the decision rule says so and holds until a reading says otherwise; alarms raised, not alarm-buckets, are what an operator sees and what we count. An alarm for a count below its band is a below-normal alarm, above it an above-normal alarm. A source may gather the rows of several senders. When each row names its sender in a field φ with value v, the sender's own count series is x_t^v, the rows at t with φ = v. The implementation counts every row of every watched value.

### 2.1 Typical level and typical window

Let L ⊂ ℤ be the learning set, the buckets admitted to learning (Section 2.5). A source's day starts at its quietest hour, the hour of the UTC day whose rows have the lowest median over the past four weeks, so that one day holds one day's traffic whatever time zone the source keeps, and a bucket's time of day s ∈ {0, …, D−1} is counted from that hour. Each day d has a kind κ(d), set by its weekday as below; a source with no weekly rhythm has one kind. The typical window of width w ending at bucket u is the median of the learned window sums that end within 30 minutes of u's time of day on the newest seven days of u's kind, among the past four weeks, that have one, where d_D(a, b) = min((a − b) mod D, (b − a) mod D) is the distance between two times of day around the clock,

```latex
M_w(u) = \operatorname{median}\{\, S_w(v) : v \in K_t(u),\ d_D(v, u) \le 6,\ \{v - w + 1, \dots, v\} \subset L \,\},
```

where K_t(u) holds the buckets of those seven days before t; a window that reaches into the day before takes only days whose day before was of the same kind as u's. A time of day with a learned window on fewer than three such days has no typical window and is not judged. The typical level of a time of day is its typical 5-minute window, m_t = M_1, the median of about 91 buckets; it is what the charts draw. A sum of per-bucket medians would be zero on a source whose rows fall in a third of its buckets, while the window that holds them is not (Section 5.4). Every window of the history is set against the typical windows as of t, and they are refreshed hourly; they move slowly.

A source whose rows differ by weekday, an office's working days against its weekends, has two kinds of day or seven. Two kinds split the weekdays by whichever of the 63 splits puts each day closest to its kind's median hourly profile; seven make each weekday its own kind. A grouping is kept only where it predicted each of the past four weeks, held out in turn, better than one kind did: each held-out day is predicted hour by hour, in log(1 + rows), by the median of the other weeks' days of its kind, and where both groupings pass, the one that predicted better in all is kept. A rhythm that is not there passes each held-out week about as often as a coin toss, so asking for all four lets it through about once in sixteen. A day with more than a tenth of its buckets missing or unlearned is left out, so an outage does not read as a kind of day, and a grouping is tried only on fourteen such days or more. The days before matter for a window that reaches into them: a Monday's 1-day window holds most of Sunday, and set against working days that followed working days it would read low every week. Section 6.7 gives the forms tried.

### 2.2 Ratios and band

Every learned window of the past H_w days is expressed as a ratio to the typical level of its time of day, as of t,

```latex
\rho_w(u) = \frac{S_w(u) + 1}{M_w(u) + 1}, \qquad t - H_w D \le u < t,\ \ \{u - w + 1, \dots, u\} \subset L .
```

The +1 keeps a time of day that is usually empty from dividing by zero and gives it a ratio like any other. Dividing by the typical level is what makes the windows of different days exchangeable enough to pool (Appendix A); the denominators are estimated from the same days as the numerators, so the ratios are exchangeable only approximately. The history H_w is 7 days for the widths up to 30 minutes and 28 days for the longer ones: a week holds about 2,000 distinct 30-minute windows but only 7 distinct days, and the quantile of a width needs enough windows that do not overlap to be a quantile at all (Section 5.4). Let q_w^-(t) and q_w^+(t) be the empirical quantiles of the ratios at levels (1−p)/2 and (1+p)/2, with band percent p = 99.9%, linearly interpolated between the two nearest order statistics, at position 0.0005·(n − 1) among n ratios: with a week of 30-minute windows, n ≈ 2,016, the lower edge lies between the second- and third-lowest ratio, and with 28 days behind a long width, n ≈ 8,064, between the fifth- and sixth-lowest, of which only about 28 do not overlap (Appendix A). With margin δ the band at t is

```latex
\ell_w(t) = (1 - \delta)\,\big[(M_w(t) + 1)\, q_w^-(t) - 1\big]^+, \qquad
h_w(t) = (1 + \delta)\,\big[(M_w(t) + 1)\, q_w^+(t) - 1\big].
```

Balanced, the preset every number not marked otherwise was measured at, uses δ = 0.3 (Section 2.7). The quantiles are refreshed hourly with the typical windows. A width is judged only once its learned windows reach back a week, so every weekday has been learned once before any reading can fall below its band; before that it has no band. The replay scores from day 8 or day 29 (Section 3), by which time every series but five has a week behind it; those five short series begin with up to 18 empty hours and are judged that much later.

### 2.3 Five widths, each where the source fills it

The judge is applied at five widths. Four count rows: w ∈ {1, 6, 72, 288} buckets, the last 5 minutes, 30 minutes, 6 hours and 1 day, each with its own ratios and quantiles over the same learning set. The fifth counts time: the gap g(t), the number of empty buckets since the last row, with its own history of completed spells (Section 2.8). Each width is judged only where the source gives it enough data for its edge to mean anything. A count width is judged at t when its typical window holds at least B = 10 rows, M_w(t) ≥ B, because a window that is usually near empty has a lower edge of zero, and a reading of zero is not below it. The gap is judged once the source has been watched 28 days and a spell has completed, and in its first year an allowance, not a gate, keeps it cautious while its history is short (Section 2.8). The gate is what makes the widths follow the source. A source with rows in every bucket is judged at all four count widths and has no spells; a source with a few rows an hour has no 5-minute or 30-minute reading and is judged at 6 hours, 1 day and the gap; a source with a row an hour is judged at 1 day and the gap; a source with a row a day is judged by the gap alone. Each width sees different drops. The 5-minute reading is below its band only when the count is near nothing, which makes it both fast on a stop and silent otherwise. The 30-minute window averages six buckets, so a shallow sustained drop falls below ℓ_6 when it never falls below ℓ_1. The 6-hour and 1-day windows catch a drop to 30% or a halving by the end of the shift or the day, which no short width catches at all. The gap sees one drop, the stop, and sees it on sources where no window does.

### 2.4 Decision rule

Let b_w(t) = 1 if S_w(t) < ℓ_w(t) and 0 otherwise, defined only where every bucket of the window is present, a band exists and the width is judged; and let b_g(t) = 1 if the current run of empty buckets exceeds the gap's edge of Section 2.8, defined only where the gap is judged. With wait k on the count widths, 4 at Balanced, and one reading on the gap, the source is below normal at t when

```latex
\exists\, w \in \{1, 6, 72, 288\}:\quad b_w(t) = b_w(t-1) = \dots = b_w(t-k+1) = 1, \qquad \text{or} \qquad b_g(t) = 1 .
```

The alarm is raised at the first such t; in the replay a count alarm ends at the first t at which no count width has k consecutive readings below, a gap alarm at the first row; the decision at t is a function of the stored series alone. The wait is where the two coordinates differ, and for a reason: a count reading at the edge flickers, so k in a row are asked of it, while the gap grows every bucket on its own, so a second reading of it says nothing the first did not. A stop is caught at the k-th empty reading of the narrowest judged count width, at Balanced 20 minutes at 5 minutes and 45 minutes at 30 minutes, within about four hours or a day at the long widths, whose readings are judged every 5 minutes and so fall below their band as soon as the stop has removed enough of the window, and by the gap when the silence has outrun the source's longest normal one by the margin. Above normal is judged at w = 6 only, by S_6(t) > h_6(t) k times in a row, and is reported as information: a rise needs no fast detection, and a second width would pay its own false alarms for a message nobody acts on in five minutes.

### 2.5 Learning set

What an alarm has judged abnormal is not learned as normal, and for the count widths that rule is a set of buckets. A bucket is in L unless it lies under a below-normal count alarm: from the alarm's rise to its end, the buckets of the windows of the widths that fired at each alarmed reading, and the same for the k−1 readings that waited before the alarm was raised. L is one set shared by the four count widths, and a window of any width that contains an unlearned bucket is not learned at that width. A 5-minute alarm that removes an hour of buckets therefore removes the hour from the 5-minute width's history and, from the 1-day width's, every day-long window that overlaps the hour, about 300 of its 8,000; a width that did not fire keeps the rest of its history and its band stands on what remains. A window is learned only once w + k buckets have passed since it ended, because until then a window it shares a bucket with may yet alarm and unlearn it: a drop that takes a day's window to show would otherwise teach the 1-day width its own first hours, and never fall under the band (Section 6.4). Buckets under an above-normal alarm are in L: more is the new normal until proven otherwise. The consequence is an asymmetry in drift. Upward each width follows the source once w + k buckets have passed. Downward it follows only drops the count widths did not alarm on, since a reading below ℓ that never reached k in a row is learned like any other. A drop the count widths did alarm on is never learned while it lasts, so a dead or halved source stays alarmed until it recovers or an operator marks it Normal or resets it.

The gap learns by the same rule in its own coordinate. Its history is the lengths of completed spells, and a spell the gap alarmed on is not learned, so a silence it called a stop never becomes a normal spell. The two histories are separate: a count alarm removes no spell and a gap alarm removes no bucket, so neither coordinate's alarms feed the other's, and a source alarmed in one coordinate goes on learning in the other. An operator's reset of the history empties both.

How an alarm ends differs between the replay and the application, and the difference touches no counted alarm. On a dead source the count widths lose their typical window: M_w is a median over the learned windows of seven days, and once none of them is learned the width has no typical window and no reading, the long widths first, since a day-long window overlaps more unlearned buckets than a 5-minute one. In the replay a count alarm ends when no width judges the source; in the application a below-normal alarm ends only on a reading inside the band, or when an operator marks it or resets the history (Section 8), so a source that has gone quiet stays reported. A gap alarm ends at the first row in both. A false alarm is counted when raised, on stretches where the source is healthy and no width loses its band, and a stop is scored by the delay to its first alarm, so the numbers of Section 4 are the same under either ending.

### 2.6 Senders

A sender behind a source is a value of a field, and the detector watches a value when its silence would be an event. A value is watched when, on each of seven days in a row, it has rows in every hour: its longest run of empty buckets is at most 12. A field names no sender once it holds more than 256 values. The cap bounds memory; the hour is what makes a value a sender, since a sender that reports all the time is one whose silence is an event, and a value quiet for an hour somewhere in a normal week would alarm every time it was quiet again. The test is a predicate, not a score: a value is watched or it is not, and a value the test refuses is not judged at all. An operator may name a field to watch or to ignore, may mark a sender's alarm Normal when its silence is intended, and a watched value leaves the watched set once a week passes without such a run. Section 4.5 scores this rule on the fields of real logs and on modeled mixes, and shows why the unit is the value and not the field: nearly every field has one rare value, and a rule that watches a field only when every value passes watches almost nothing.

A watched value is judged by the judge of Sections 2.1 to 2.5 applied unchanged to x_t^v, its own 5-minute series, at the four count widths and at Quiet (Section 2.7), because a per-sender false-alarm rate is paid once per sender. The gap is not applied to it. A watched value has rows in every hour, which put at least 24 in a day's window, so the 1-day width judges it at every hour and the 30-minute width wherever the value is dense enough, which is where its stop is caught within the hour. A gap beside them would be a second alarm paid per sender, and the one gap replayed on senders, kept per time of day, raised 5 to 17 false alarms per sender-month (Section 6.8). The cost falls on a sender that is thin, whose stop waits for a longer width: on the modeled senders at a row a minute, whose 5-minute width is gated out, {{senders_thin_by60_00_quiet}} of stops are caught within the hour and {{senders_thin_by360_00_quiet}} within 6 hours (Section 4.5).

### 2.7 Presets

The margin δ and the wait k are the two levers an operator sets, and everything else is fixed. A setting is a pair of margin and wait; a preset is one of four named settings. An operator picks one preset per source, and another for its senders:

{{SETTINGS_SHORT}}

A stop is caught at every preset, at the k-th empty reading; partial drops and false alarms move together along them, and Section 5.2 gives the frontier the four sit on. Balanced is a source's preset until an operator picks another, and every number not marked otherwise was measured at it; the real logs (Section 4.2) give all four. Quiet is the senders' preset, because a per-sender false-alarm rate is paid once per sender, and a source with a hundred senders has a hundred chances for a dip to alarm (Section 4.5). Fast catches a stop at the first empty reading and pays for it with a wide margin, which gives up most halvings; Sensitive keeps the narrow margin and catches the most degradations for the most false alarms.

### 2.8 The gap

The gap is the judge in the coordinate of time per row. Its datum is a quiet spell, a maximal run of empty buckets, measured by its length: a source with a row in most buckets has spells of one or two buckets, a source with a row an hour spells of about twelve, a source with a row a day spells of about three hundred. Its history is the lengths of the spells that completed in the past year, lay over buckets with data and did not lie under one of the gap's own alarms, pooled raw, with no typical level for the time of day. The pooling has a price: on a source with a daily shape the record is a night spell, so a daytime stop is confirmed only once it has outlasted the longest night silence by half again. The price is paid because the alternative was measured and lost: a record kept per time of day leaves each hour a handful of spells to beat, and on a modeled source of a hundred thin senders it raised 5 to 17 false alarms per sender-month, 455 to 1,750 per source-month (Section 6.8).

Its quantile is the maximum of the history, L, and the length of the history is what sets its false alarms. A healthy source whose spells are exchangeable outlasts all n before it with probability at most 1/(n + 1) (Proposition 1), and a source that makes λ spells per unit of time has about λT of them in a history of length T, so it sets a new record at most about once per T, whatever the law of its spells (Corollary 3). A burst adds spells, but short ones that never hold the record, so a source that fires in bursts is bounded by its long spells alone. The history is therefore as long as the false-alarm period accepted per source, a year. The count widths' 28 days hold two spells of a source that fires every two weeks, and on such a source the gap would set a record about every other spell (Section 4.7).

Its edge is m·L with margin m = 1.5, upward, since a long gap is the failure where a low count is. The margin is what lets the record grow. A spell the gap alarmed on is not learned, so at m = 1 nothing longer than the record could ever be learned, the record would only fall as old spells left the year, and every spell over it would alarm again (Section 6.9); at m = 1.5 a spell up to half again the record is learned and becomes the record. The margin also divides the false alarms, the more the more spells a source has: for spells that end at a steady chance, the chance of outlasting 1.5 times the longest of n is about 1.33/(n√n) (Corollary 3).

A year is not in hand when a source is first watched. Until it is, the gap takes as its record the longest spell so far plus μ̂ ln(T/w), where T is a year, w the time watched and μ̂ = Σs² / 2Σs over the learned spells. For spells that end at a steady chance μ̂ estimates their mean, and the longest of n grows by the mean each time n grows e-fold, so the sum is the record a year would be expected to hold; a burst's short spells barely move μ̂, since each spell counts by its square. The allowance stands where a gate would: with little history it is large and the gap is cautious, and it shrinks to nothing at a year. The gap judges once the source has been watched 28 days, the count widths' history, and a spell has completed. The source is stopped at t when its current run of empty buckets exceeds m·L, at the first such reading, and the alarm ends at the first row.

What the gap costs and what it cannot do follow from its coordinate. It stores the end and length of each spell of a year, up to about eight thousand for a source with a row an hour, a few dozen for one that is silent for days, and none for a dense source with no empty bucket, which it does not judge. What storage cannot buy is speed: a stop is confirmed only when the silence has outlasted the longest normal one of the year by the margin, about an hour at a dozen rows an hour, most of a day at a row an hour, a week or more on a source that is silent for days and about two months on one that fires every two weeks at random, and no setting changes that, because the source's own habit sets the edge. Nor does the gap see a degradation: a source that halves its rate has longer spells than before, but seldom one longer than its record, so a drop that leaves rows is the count widths' to catch, where they judge. The five widths are judged on every source, each where its gate admits it, and a stop is whichever fires first.

## 3. How it was measured

*Baseline.* The baseline judges S_6(t) against the 99% band of the about 91 windows at the same time of day over the past week, with linear interpolation, alarming at once on a reading outside; windows under an alarm on either side are not learned; judging starts once three days have a window. The implementation has one further trait, reproduced here: an alarm begins a moment after the window that caused it ended, and a window counts as alarmed only if it ends after the alarm began, so the first window of every incident is learned.

*Real count series.* Fourteen count series from the Numenta Anomaly Benchmark (Lavin and Ahmad, 2015; MIT license) with its labeled incident windows: ten Twitter mention volumes (5-minute counts, 56 days, medians 0 to 50 a bucket, judged from day 29); New York taxi passengers (30-minute counts spread evenly over 5-minute buckets, seven months, judged from day 29); an AWS load balancer's request count and two EC2 network-in series (5-minute, 14 to 17 days, judged from day 8). Two collapses the archive does not label are labeled here, after the replay's alarms found them: EC2 257a54 falls to a third of its level as its labeled spike ends on day 6 and stays there to the end, which leaves it no judged time, and the load balancer's requests read under their usual level in every quarter of a day from day 9 to noon on day 11.

*Real logs.* Ten public logs with a timestamp on every line, counted into 5-minute buckets: four web server access logs of 1994 to 1995 from the Internet Traffic Archive (Arlitt and Williamson, 1996), NASA Kennedy Space Center over two months, ClarkNet over two weeks, the University of Saskatchewan over seven months and the University of Calgary over a year; five system logs from Loghub (He et al., 2020), an OpenSSH server over 28 days, a Zookeeper service over 27 days, a Linux syslog and an Apache error log over 264 days each, and the Blue Gene/L supercomputer's RAS log over 215 days; and the job events of a Google compute cluster over 29 days (Reiss, Wilkes and Hellerstein, 2011). Medians run from 0 to 784 lines a bucket. A run of two hours or more with no line at all, on a log that usually has lines in every bucket, is an outage the archive documents, as NASA's shutdown for a hurricane, or a gap in the collection, and is masked like a labeled incident. Logs of 35 days or more are judged from day 29, shorter ones from day 8. A log's fields, the status code and method of a request, the level and component of a message, the event type of a job, are the fields the sender rule is scored on.

*Modeled series.* Seven shapes, three seeds each, 56 days: a level, a daily and weekly shape, and multiplicative noise that follows an AR(1) process in log space with about 50 minutes of memory, drawn as Poisson counts. Judged on days 29 to 56.

| Shape | Count per 30 min | Noise σ |
|---|---|---|
| Steady | 1,200 | 0.1 |
| Variable | 1,200 | 0.6 |
| Daily | 360 to 2,040 over the day | 0.3 |
| Thin daily | 1 to 35 over the day | 0.3 |
| Weekday/weekend | Daily on weekdays, a fifth of it at weekends | 0.3 |
| Steady, falling 3% a day | 1,200, falling | 0.1 |
| Variable, falling 3% a day | 1,200, falling | 0.3 |

*Sparse sources.* Four Poisson sources with no daily shape, at 12, 4 and 1 rows an hour and 1 row a day, two seeds each, 84 days judged from day 29, scored at horizons to ten days.

*Sender mixes.* Eight mixes of 3 to 100 senders behind one source, each a daily shape with AR(1) noise at about 7, 2 or 1 counts a second or 1 a minute; two seeds; 56 days judged from day 29. Each sender's series holds every one of its rows, as the implementation counts them (Section 8).

*False alarms.* Alarms raised per 30 days of judged time outside the labeled incidents; for a sender, per 30 days of that sender's judged time. A bucket is judged when at least one width has a band and is gated in there. An alarm is counted when raised, so a band that flickers pays for every flicker. A rate over a set of series is the mean of the per-series rates, each series weighing the same; we also give the count of alarms and the source-months judged.

*Drops detected.* At every judged bucket the count is cut to a fraction f ∈ {0, 0.1, 0.3, 0.5} of its level from that bucket on: a stop, a drop to a tenth, a drop to 30%, a halving. The drop is detected within τ if the detector alarms within τ of the cut, following the band as learned up to the cut; we report τ = 20 minutes, 1 hour, 6 hours and 1 day, and the median delay. The cuts are injected at every other judged bucket that is not under an alarm, so neighboring cuts share the band and the noise and are far from independent; the labeled incidents of the real series serve only to exclude windows from the false-alarm count. The margin and wait were chosen on the real count series (Section 5.2), so their numbers are in-sample; the real logs were added after the band was fixed and are out of sample for it. The gap's margin was chosen with the logs in view (Section 4.7), so for the gap the logs are in-sample too.

*Replay.* One harness implements the detector as smeltr ships it, at its four presets, its own alarms setting its own learning set; every number in Sections 4 and 5 that is not marked otherwise comes from it. Each series is placed at its own UTC time of day, as a leader sees it, so its days start at its quietest hour as they do in the application. A second harness implements the baseline, the twelve bands of Section 6.1 and the sweep of Section 6.2, keeping the baseline's judging and learning rules throughout: windows under an alarm on either side are not learned, and the band is refreshed every reading. Every run is seeded and was reproduced row for row.

## 4. Results

The results follow the sources from dense to thin, because that is the axis the detector follows. The count series (4.1), the logs (4.2) and the modeled shapes (4.3) show the band where it has rows to judge; the sparse sources (4.4) show where the long widths take over and where even they fall silent; the senders (4.5) show the same band on the values behind a source; each width's own part is separated out (4.6); the gap is scored where the count widths are not (4.7); and the five widths are scored together on every source (4.8), which is where the claim that every stop is caught is made good.

Balanced throughout: widths 5 minutes, 30 minutes, 6 hours and 1 day, 7 and 28 days of history, a 10-row gate, day groups, δ = 0.3, k = 4. "Months" is the source-months judged: buckets at which at least one width had a band and was gated in. Every share is of the drops injected at judged buckets.

### 4.1 Real count series

Fourteen series, sorted by how much they carry. "Median" is the median count per 5-minute bucket. False alarms are per source-month, baseline and Balanced; the rest is the share of injected drops caught at Balanced within the horizon.

{{REAL}}

The series fall into three classes. On the eight judged series with a median of 8 or more a bucket every stop is caught within the hour but on EC2 5abac7, {{s:shipped.json:EC2 network, 5abac7:2:by60_0.0}}, whose bursts set its band (Section 7), and most within 20 minutes on all but Tweets KO; most drops to a tenth are caught within the hour; what is given up within the hour is drops to 30% and halvings, and most drops to 30% are caught by the end of the day at the long widths. Three of the real false alarms are here: two on NYC taxi, which come with day groups (Section 5.4), and one on Tweets AAPL. On the three series at 2 or 3 a bucket, IBM, CRM and UPS, the 5-minute width is gated out and a stop waits for the 30-minute width: {{s:shipped.json:Tweets UPS:2:by60_0.0}} to {{s:shipped.json:Tweets IBM:2:by60_0.0}} are caught within the hour and all within the day. The last false alarm is on IBM, which makes {{alarms_bal_real}} in {{months_bal_real}} source-months: {{real_alarm_series}}. On the two series with a median of zero, PFE and CVS, the short widths are gated out and only the long ones judge; every stop is caught within the day, where the baseline, judging at 30 minutes only, caught {{b:nab:realTweets/Twitter_volume_PFE:caught_0.0}} and {{b:nab:realTweets/Twitter_volume_CVS:caught_0.0}} of their stops within the hour. On PFE and CVS the gap is faster than the long count widths, and a stop is confirmed by one or the other at a median of {{e:nab:realTweets/Twitter_volume_PFE:median_either}} and {{e:nab:realTweets/Twitter_volume_CVS:median_either}}; on IBM, CRM and UPS the 30-minute width is the faster (Section 4.8). EC2 257a54 has no judged time and no row of numbers.

### 4.2 Real logs

Ten logs the band was not designed on, sorted by how much they carry; "median" is the median lines per 5-minute bucket, and false alarms are given at all four presets.

{{LOGS}}

Four logs have lines in nearly every bucket. Three of them, ClarkNet, the Google cluster and NASA, behave like the dense count series: every stop within the hour on the first two and {{s:shipped_logs.json:NASA web, Jul-Aug 1995:2:by60_0.0}} on NASA, most drops to a tenth within the hour, and {{q_clarknet_alarms}}, {{q_google_alarms}} and {{q_nasa_alarms}} alarms; ClarkNet's one alarm in 0.2 months is its {{q_clarknet_band}} a month at every preset. The university servers of 1994 and 1995 drop with the week and the academic calendar: Saskatchewan at 31 lines a bucket raises {{q_sask_band}} a month and catches every stop within the hour; Calgary at 3 a bucket is judged mostly at the long widths, catches its stops within the day, and raises {{q_calgary_band}} a month, the most of any web log. On these the presets spread widest, Saskatchewan from {{s:shipped_logs.json:Saskatchewan web, Jun-Dec 1995:0:false}} a month at Fast to {{s:shipped_logs.json:Saskatchewan web, Jun-Dec 1995:3:false}} at Quiet. The four system logs have an empty median bucket, and the band judges them only where a typical 6-hour or 1-day window holds ten lines: {{s:shipped_logs.json:BGL supercomputer, 215 days:2:by1440_0.0}} of stops are caught within the day on BGL, {{s:shipped_logs.json:Linux syslog, 264 days:2:by1440_0.0}} on Linux and {{s:shipped_logs.json:Apache error log, 264 days:2:by1440_0.0}} on the Apache error log, whose quiet weeks fill no width, and on those three the false alarms run {{q_apache_band}} to {{q_bgl_band}} a month, because a log of errors has no normal rate for a ratio to be taken against, and its quietest week sets a band the next quiet week falls under. Some of the Apache alarms stand on silences of 43 to 168 hours, the record's longest completed spells (Section 9), which are stops by any reading and count as false only because the archive does not label them. Zookeeper, 27 days of lines in bursts, fills no width and is not judged. Section 4.7 scores the gap on the same logs and Section 4.8 all five widths together.

### 4.3 Modeled series

Seven shapes, three seeds each, 2.8 source-months per shape.

{{MODELED}}

Where the count is steady the band is tight: every drop to 30% is caught within the hour and {{s:shipped.json:steady:2:by60_0.5}} of halvings, the rest by the end of the day, which is the margin's price against a 10% noise. The variable shape, σ = 0.6 in log space, has windows at a quarter of the typical level inside its own week, so a drop to a tenth is mostly caught and a drop to 30% mostly not within the hour, and it carries the modeled false alarms, {{mod_alarm_series}}. On the weekday/weekend shape, a weekend day at a fifth of a weekday, day groups give each kind of day its own typical window, and every drop to a tenth is caught within the hour, where one kind of day for the whole week catches {{s:shipped.json:weekday/weekend:6:by60_0.1}} (Section 5.4). The thin daily shape, 1 to 35 rows a half hour, is gated out of the 5-minute width and, at night, the 30-minute one: its stops and its drops to a tenth wait for a width that holds rows, and {{s:shipped.json:thin daily:2:by60_0.0}} of the stops and {{s:shipped.json:thin daily:2:by60_0.1}} of the drops to a tenth are caught within the hour.

### 4.4 Sparse sources

Four Poisson sources with no daily shape, from 12 rows an hour to one a day, 84 days, two seeds. Three detectors at Balanced: the two short widths alone; the four widths with 7 days of history behind every width; the four widths with 28 days behind the long ones, as shipped.

{{SPARSE}}

A source with a dozen rows an hour has 1 row in a typical 5-minute bucket and 6 in a typical half hour, and the short widths are gated out or near it; a source with 4 rows an hour fills only the 6-hour and 1-day widths, and one with a row an hour the 1-day width alone. With the long widths every stop on the first three is caught, within 6 hours on the first two and within the day on the third, at a median of {{sparse_median_12}}, {{sparse_median_4}} and {{sparse_median_1}}. The 28 days of history behind the long widths buy nothing here and cost little: the source with a row an hour raises {{sparse_false7_1}} false alarms a month with 7 days behind them and {{sparse_false28_1}} with 28, and catches {{sparse_by360_7_1}} and {{sparse_by360_28_1}} of its stops within 6 hours, every one within the day either way. A Poisson source has no shape, and its worst day of seven is near its typical day. The history earns its place on the real series, whose days differ, where 7 days behind every width raised {{c:shipped_hist7.json:nab:0:false}} alarms against {{alarms_bal_real}} (Section 5.4). A source with one row a day fills no width up to a day and is not judged by the band; a week-wide width was tried, and the gate admits it only in the weeks that hold ten rows, {{sparse_week_months}} of the {{sparse_months_day}} months the other sparse sources are judged; there it raised {{sparse_week_false}} alarms and caught a stop after a median of {{sparse_week_median}}. Its stop is the gap's to catch (Section 4.7).

### 4.5 Senders

The detector on each sender's series at Quiet, two seeds of each mix, with Balanced's false alarms beside it:

{{SENDERS}}

Every sender's stop is caught within 6 hours in every mix, and within the hour in every mix but two: the senders silent from 1 to 5 a.m., a mix the watch test of Section 2.6 refuses, scored to show a sender silent for hours, whose short widths are gated out at night; and the senders at a row a minute, whose 5-minute width is gated out and whose stop waits for the 30-minute one. At Quiet the senders raise {{c:shipped_senders.json:all:0:false}} false alarms in {{mo:shipped_senders.json:all:0:false}} sender-months, on the mix at a row a minute. At Balanced the mean is {{m:shipped_senders.json:all:1:false}} per sender-month, {{senders_false_bal_lo}} to {{senders_false_bal_hi}} on the five steady mixes, and a per-sender rate is paid per sender: a source with a hundred senders at 0.1 each alarms ten times a month. That is why senders are judged at Quiet (Section 2.7), and what it gives up is the degradation: stops within the hour are {{senders_by60_00_quiet}} at Quiet and {{senders_by60_00_bal}} at Balanced, drops to a tenth within the hour fall from {{senders_by60_01_bal}} to {{senders_by60_01_quiet}}, and halvings within the day from {{senders_by1440_05_bal}} to {{senders_by1440_05_quiet}}. A sender at Quiet is watched for its stop and its collapse, not for its halving.

Which fields of a record identify senders, by the criterion of Section 2.6, on seven modeled fields of one source, three seeds:

| Field | Values | Longest silence in the week | Watched |
|---|---|---|---|
| sensor, 10 steady senders | 10 | none | yes |
| region, 50/30/20% of rows | 3 | none | yes |
| method, 70/25/5% of rows | 3 | none | yes |
| status, errors in bursts a few times a day | 2 | 20 to 36 hours | no |
| employee, 120 people on three shifts, weekdays | 120 | 66 to 67 hours | no |
| user, 2,000 values | 2,000 | past the cap | no |
| build, one value per day | 7 | 6 days | no |

The same criterion on the fields of the real logs, over the week before judging starts:

{{FIELDS}}

A field-level form of the rule, under which a field is watched only when every one of its values is seen every hour, watches {{fields_watched}} of {{fields_total}} fields: nearly every real field has a rare value, a 500 status or a PUT, that is silent for hours in a normal week, and one such value fails the field. The rule of Section 2.6 watches each value on its own and admits {{values_watched}} values; the detector on each of their series, at Quiet as every sender is judged, gives:

{{VALUES}}

On the values the per-value form watches the detector behaves as it does on a source. A value with rows in every bucket, GET, 200, the Google event types, is caught within the hour on every stop, at its log's own false-alarm rate at Quiet. A value with an empty median bucket, ClarkNet's HEAD and every OpenSSH kind, is judged at the long widths, nearly always caught within the day, and on OpenSSH pays {{values_openssh_lo}} to {{values_openssh_hi}} false alarms a month. The hour test is right about what a sender is, and the unit it applies to has to be the value: a field is almost never a sender, because nearly every field has a rare value. A value the test admits that still has an empty median bucket, as OpenSSH's kinds, is judged at the long widths like a thin source and pays the thin source's false alarms; a value the test refuses is not watched, and its stop goes unseen, which is the rule's cost.

### 4.6 Each width's share

Each width alone, the two short ones, and all four, on the real and modeled count series:

{{WIDTHS}}

Each width is judged on the series and the buckets that fill it, and a share is over the drops injected at the buckets that row judges, so the rows have different denominators and do not add. "5 and 30 minutes" is scored on the {{j:shipped_widths.json:nab:4}} real series and the hours those widths judge; "All four" also on the thin series' nights and on the series only the long widths judge, where no stop is caught within the hour, which is why its stops within the hour read {{widths_hour_gap}} points lower than the two short widths' and its stops within the day read higher. On the buckets both rows judge the four widths catch what the two do: the long widths add readings, and a reading can only add an alarm. The false-alarm rates are likewise per month that row judges. The 5-minute width alone is judged on {{j:shipped_widths.json:nab:0}} of the 14 real series, raises {{c:shipped_widths.json:nab:0:false}} false alarm, and is the stop's width: {{p:shipped_widths.json:nab:0:by20_0.0}} of stops within 20 minutes on the series it judges, and {{p:shipped_widths.json:nab:0:by1440_0.3}} of drops to 30% within the day. The 30-minute width alone carries the real false alarms, {{c:shipped_widths.json:nab:1:false}} of them, {{m:shipped_widths.json:nab:1:false}} a month, and most of the degradation caught within the hour. The 6-hour and 1-day widths alone catch no stop within the hour and nearly every one within the day, and {{p:shipped_widths.json:nab:3:by1440_0.3}} to {{p:shipped_widths.json:nab:2:by1440_0.3}} of drops to 30% within the day on real series; they are the day's widths. The four together take each width's catch at its own horizon, the 5-minute speed on a stop, the 30-minute rate on a drop to a tenth, the day's drops at the long widths, for {{m:shipped_widths.json:nab:5:false}} / {{m:shipped_widths.json:modeled:5:false}} false alarms a month: on the modeled series the long widths add {{widths_long_add_mod}} a month to the 30-minute width's, and on the real series the four raise the 30-minute width's {{c:shipped_widths.json:nab:5:false}} alarms, the rate reading lower because it is spread over the months only the long widths judge as well.

### 4.7 The gap

The gap scored alone, on every source on which a spell completes: the tweet series, the modeled thin daily shape, the sparse sources and all ten logs; ClarkNet, the Google cluster and one weekday/weekend seed are judged on a single spell. A dense source with no empty bucket is not judged. Every series here is shorter than a year, so the gap runs on its first year's allowance throughout. "Longest normal spell" is the longest learned spell with the allowance, averaged over the judged time; a stop is confirmed when the current spell is m times that, and the median is the time from the stop to that confirmation. The count widths' numbers on the same sources are in Sections 4.1 to 4.4, and Section 4.8 puts the two coordinates together.

{{QUIET}}

At margin 1.5 the gap raises no false alarm on any count series, modeled shape or sparse source, and {{c:quiet.json:logs:1:false}} alarms in {{mo:quiet.json:logs:1:false}} source-months on the ten logs, {{r:quiet.json:logs:1:false}} a month pooled, where 28 days of spells and a ten-spell gate raised {{c:quiet_28.json:logs:1:false}}. Most of them are on two logs. The Apache error log raises {{q:Apache error log, 264 days:1:false_count}}: from its seventh week it falls silent for most of a day to a week at a time, two to eighteen times its longest silence of the weeks before, and none of those silences is learned, because each alarmed; an operator's Normal mark on such a silence is what teaches it (Section 8). Zookeeper, a 27-day log judged from its second week, raises {{q:Zookeeper, 27 days:1:false_count}} on a record of {{q:Zookeeper, 27 days:1:spells}} spells. At margin 1 the record cannot grow past itself, and the gap raises {{c:quiet.json:sparse:0:false}} alarms on the sparse sources and {{c:quiet.json:logs:0:false}} on the logs; at margin 2 the logs raise {{c:quiet.json:logs:2:false}}. The year costs time where the source is regular: a stop is confirmed at a median of {{qh:12 rows an hour:1}} at 12 rows an hour, {{qh:4 rows an hour:1}} at 4 rows an hour, {{qh:1 row an hour:1}} at a row an hour and {{qh:1 row a day:1}} at a row a day, which no count width judges. On the tweet series it confirms a stop at a median of {{qh:Twitter_volume_IBM:1}} to {{qh:Twitter_volume_CVS:1}}, within the day on all of them. It is slowest on the Linux syslog, whose normal silences run {{q:Linux syslog, 264 days:1:longest_h}} hours with the allowance, so a stop is confirmed after {{qh:Linux syslog, 264 days:1}}: the gap cannot be faster than the source's own habit, and it says so by the number it reports.

A source that fires in bursts every two weeks is the case the year is for, and no series above is long enough to show it. The table below replays the gap on modeled sources over six years, five seeds each, beside the same gap with 28 days of spells and a ten-spell gate: three that fire about every two weeks, at random in bursts of 20 to 200 rows over one to six hours, at random as single rows, and every 10 to 18 days in bursts; and four that run steadily for four years, three seeds each. Its predictions were written before it ran (Section 9).

{{RARE}}

With 28 days the bursty source is judged a third of the time, right after a burst has filled the ten-spell gate, on a record of two spells, and raises {{rare_false_28}} false alarms a year; as single rows it is never judged. With a year it is judged nearly all the time, raises {{rare_false_year}} a year and confirms a stop after {{rare_stop_year}}: the longest of a year's spells of a source that fires every two weeks at random runs about seven weeks, and a stop can be told from such a silence only once it has outlasted it. Bursts change nothing, since the record is a spell between bursts either way. On the steady sources the year buys the bound for time: a row a day goes from {{day_false_28}} false alarms a year to {{day_false_year}} and confirms a stop after {{day_stop_year}} instead of {{day_stop_28}}, a row an hour from {{hour_false_28}} to {{hour_false_year}} and {{hour_stop_year}} instead of {{hour_stop_28}}.

### 4.8 Count widths and gap together

This is the detector as an operator sees it: five widths on every source, each judged where its gate admits it, and a stop caught by whichever fires first (Section 2.8). The count widths and the gap are scored together on every count series, modeled series, sparse source and log: a bucket is judged when either coordinate judges it; a false alarm is a start of either's alarm, the two counted once where they overlap; a stop injected at a judged bucket is caught at the earlier of the two delays, the count widths' within 25 hours and the gap's at any delay. Rates are means over the sources of a row, as everywhere. Every share is over the buckets either coordinate judges, so the count widths' own columns here read lower than in Sections 4.1 to 4.4, where their shares are over the buckets they judge alone; and a stop is injected only where neither coordinate is alarmed, so a coordinate's share here can differ either way from its own: the gap catches {{e:logs:Zookeeper, 27 days:by1440_quiet}} of Zookeeper's stops within the day here and {{q:Zookeeper, 27 days:1:by1440_0.0}} in Section 4.7. The count series on which no spell completes, which the gap never judges, are one row; the tweet series, dense by their medians but with quiet nights, are judged in both coordinates and have a row each. The rates here differ from Sections 4.1, 4.2 and 4.7 because the months differ: a source's months are the buckets either coordinate judges.

{{EITHER}}

Every injected stop on every one of the {{either_sources}} sources is caught, and the two coordinates divide the sources between them. Within the day the detector catches {{either_day_real}} of stops on the real count series, {{either_day_mod}} on the modeled series, {{either_day_sparse}} on the sparse sources and every stop on seven of the ten logs and {{e:logs:BGL supercomputer, 215 days:by1440_either}} on BGL. On the Linux syslog, whose normal silences run a day, a stop is confirmed at a median of {{e:logs:Linux syslog, 264 days:median_either}}, {{e:logs:Linux syslog, 264 days:by1440_either}} within the day; on Zookeeper, judged from its second week on a record of silences a day long, at {{e:logs:Zookeeper, 27 days:median_either}}, {{e:logs:Zookeeper, 27 days:by1440_either}} within the day. The other exception to the day is the source with a row a day, whose stop is confirmed at a median of {{e:sparse:1 row a day:median_either}}, the gap's margin over its longest silence with the allowance. On the dense count series and the dense modeled shapes the count widths catch a stop at a median of 20 minutes, and the gap, where it judges at all, is slower by hours. On the sparse sources and on the Apache error log the gap is what catches the stop within the day, at a dozen rows an hour at a median of {{e:sparse:12 rows an hour:median_either}}, where the 6-hour width takes hours. On the thin tweet series the 30-minute width is the faster where it judges and the gap holds the night.

The false alarms of the five widths together are the count widths' with the gap's added where the gap judges. The five raise {{either_false_real}} false alarms a month on the real count series, the count widths' {{band_false_real}} and nothing from the gap, and {{either_false_logs}} a month over the logs against the count widths' {{band_false_logs}}. The gap adds alarms on Zookeeper, a log under a month long that it judges from its second week on a short record, and takes some away on the Apache error log, where a silence the gap alarms on bridges two count alarms that were counted twice into one.

## 5. The trade

Section 4 said what is caught. This section says what it costs, in the order of the three needs: stops cost time and not alarms (5.1); degradations trade against false alarms, and the margin and the wait set the rate of exchange (5.2); the baseline's detection was never usable at its alarm rate (5.3); and each part of the detector has a measured reason to be there (5.4).

### 5.1 Full stops cost time, not alarms

Every preset catches the same stops on the same series: {{by1440_00_fast_real}}, {{by1440_00_sens_real}}, {{by1440_00_bal_real}} and {{by1440_00_quiet_real}} of real stops within the day at Fast, Sensitive, Balanced and Quiet, and {{by60_00_fast_real}}, {{by60_00_sens_real}}, {{by60_00_bal_real}} and {{by60_00_quiet_real}} within the hour. A stop is a reading of zero, and zero is below the band wherever the lower edge is positive. The edge ℓ_w(t) is positive exactly when q_w^−(t) > 1/(M_w(t) + 1). A ratio (S_w(u) + 1)/(M_w(u) + 1) of an empty window is 1/(M_w(u) + 1), which is at most 1/(M_w(t) + 1) when the window's time of day is at least as busy as now, so the edge is zero when the order statistics the quantile interpolates come from empty windows at times of day at least as busy as now: at the short widths, whose edge sits between the second- and third-lowest of about 2,000 ratios, two or more such windows in the history, at the long widths, between the fifth- and sixth-lowest of about 8,000, five or more. On the count series the band judges that is rare, and every real stop is caught within the day (Section 4.1); on a bursty log it is common, which is why the count widths catch {{e:logs:Apache error log, 264 days:by1440_band}} of the Apache error log's stops within the day and the gap the rest (Section 4.8). The gate does not make the edge positive; it removes the widths whose windows usually hold too few rows for their ratios to mean anything. Where the edge is positive, a stop is below the band at the first reading that no longer holds the rows before the stop. What the preset changes is the wait, k readings, and what the source changes is the width: the stop is caught about k readings after the narrowest judged window has emptied, 20 minutes on a dense source at Balanced, 45 minutes where only the 30-minute width is judged, hours at the long widths. The operator picking a preset is not trading stops away. The operator picking a source's sampling, how many rows it emits, is choosing how soon its stop can be seen.

### 5.2 Degradations trade against false alarms

The margin and the wait are the two levers an operator sets. At the shipped widths, history, gate and day groups, every pair of δ in 0.2, 0.3 and 0.5 and k in 1, 3, 4 and 6:

{{GRID}}

The wait is the cheaper lever. From k = 3 to k = 4 at δ = 0.2 the false alarms fall from {{m:shipped_grid.json:nab:1:false}} to {{m:shipped_grid.json:nab:2:false}} a month on real series for two points of drops to a tenth and four of drops to 30% within the hour, and 5 minutes on a stop. From δ = 0.2 to 0.3 at k = 4 they fall from {{m:shipped_grid.json:nab:2:false}} to {{m:shipped_grid.json:nab:6:false}} for four and five points. From δ = 0.3 to 0.5 at k = 4 they fall only to {{m:shipped_grid.json:nab:10:false}}, for eight points of drops to a tenth and fourteen of drops to 30%, and halvings caught within the day go from {{p:shipped_grid.json:nab:6:by1440_0.5}} to {{p:shipped_grid.json:nab:10:by1440_0.5}}: past δ = 0.3 the margin buys little quiet and gives up the halvings. At k = 1 a narrow margin alarms {{m:shipped_grid.json:nab:0:false}} times a month, and Fast takes δ = 0.5 to bring that to {{m:shipped_grid.json:nab:8:false}}. Stops within the hour lose a point for each step of the wait and nothing to the margin. The cost column at K·λ = 30, computed from the unrounded shares, runs from {{m_cost_lo}} to {{m_cost_hi}} on real series: a point of the mean share missed adds 0.3 to it and an alarm a month adds 1, so a difference of 0.5 is about 1.7 points of the share missed or half an alarm a month, and the rule below treats a difference under 0.5 as a tie and one of 0.5 or more as a ranking.

An operator who knows how often a source really drops, λ a month, and what a missed drop costs in false alarms, K, picks the setting that minimizes F + K·λ·M, M the share missed. Only the product K·λ matters; the last column is that cost at K·λ = 30, a drop a month with a miss worth 30 false alarms, or a drop a quarter worth 90, with the share missed taken as the mean over stops, drops to a tenth and drops to 30% within the hour. The rule picks the quietest setting whose cost is within 0.5 of the least (Section 9). On the fourteen real series the least is 13.8, shared by δ = 0.2 with k = 3 or 4 and by δ = 0.3 with k = 3; δ = 0.2, k = 6 and Balanced, δ = 0.3, k = 4, are within 0.4 of it, and Balanced is the quietest of the five, so the rule picks Balanced, and the modeled series alone pick it too. Split into two halves of seven by name, the real series do not agree: half A picks δ = 0.3, k = 3 and half B picks δ = 0.2, k = 4, and Balanced misses the tie by under a tenth on each, 0.58 above the least on half A and 0.51 on half B. Each half's pick is a neighbor of Balanced, one step lower in the wait or in the margin, and each scores worse on the other half than that half's own pick: half A's pick raises 0.53 false alarms a month on half B where half B's raises 0.18, and half B's raises 1.34 on half A where half A's raises 0.38. Seven series are too few to separate the settings around Balanced; on fourteen, Balanced is the quietest of the five within the tie. Sensitive is the least-cost setting that the tie gives up, for an operator who weighs a missed degradation more than K·λ = 30 does; Quiet is for one who weighs a false alarm more, and for senders (Section 2.7), and Section 5.3 shows a model under which that weighting is the right one.

The four presets of Section 2.7 on the frontier:

{{SETTINGS}}

### 5.3 Why the baseline's detection is on paper only

The baseline's {{base_tenth_real}} of drops to a tenth rests on {{base_false_real}} false alarms per source-month, about {{base_per_day}} a day per source. An operator who acts on every alarm acts on a false one {{base_false_share}} of the time when a source drops once a month; one who acts on none misses every drop. Model the operator as acting on an alarm with probability equal to its precision, λR/(λR + F), where λ is the rate of real drops, R the share the rule detects and F its false-alarm rate: then the share of drops acted on is R times that precision. For a source that drops to a tenth once a month, the baseline's precision is {{base_precision}} and it gets {{base_acted}} of the drops acted on; Balanced's precision is {{precision_bal}} and it gets {{acted_bal}} (Section 9). Under this model a quieter setting is better until it misses more than its quiet buys: across the grid the most is acted on at {{acted_best_setting}}, {{acted_best}}, between Balanced and Quiet; Quiet gets {{acted_quiet}}, Balanced {{acted_bal}}, Sensitive {{acted_sens}} and Fast {{acted_fast}} (Section 9). The two models disagree at the margin, the cost column of Section 5.2 putting Balanced 0.4 above its least and this one {{acted_gap}} points below its best, because this one prices a false alarm by the operator's fatigue and scores one drop size where the cost column weighs three. Balanced follows the cost column; Quiet is the preset for an operator whose alarms are read by people, and for senders, where a source with a hundred senders has a hundred chances for a dip to alarm (Section 2.7). The replay cannot supply λ, nor the cost of a missed drop; what it can say is that at the baseline's rate every detected drop is one alarm among about {{base_one_in}}.

### 5.4 What each part contributes

The detector is two short widths, 5 and 30 minutes on 7 days of history, plus four parts: the long widths, the 28 days of history behind them, the gate with the typical window it tests, and day groups. Each is removable, and the table gives what removing it costs. Each row judges its own bucket set and its shares are over that set: the row without the gate judges the thin series at the short widths too, and the row without the long widths judges only where a short width passes the gate, so their shares are over buckets the shipped row does not judge or leaves out ones it does.

{{PARTS}}

The long widths buy the day and the thin series. Without them drops to 30% caught within the day fall from {{by1440_03_bal_real}} to {{by1440_03_two_real}} on real series and halvings within the day from {{by1440_05_bal_real}} to {{by1440_05_two_real}}, and the thin tweets are judged only where the 30-minute width passes the gate, {{months_two_real}} months against {{months_bal_real}}; that row's stops within the hour read higher because the thin hours it leaves out are where stops are slow. They raise no alarm of their own: the row without them raises the same {{alarms_two_real}}. They need their 28 days because a width's quantile is only as good as the number of windows that do not overlap behind it: a week holds 2,000 half-hour windows and 7 days, and a 1-day width learned from 7 days alarms on the first day that is 20% under the worst of the week. With 7 days behind every width the real series raise {{c:shipped_hist7.json:nab:0:false}} alarms, {{m:shipped_hist7.json:nab:0:false}} a month, against {{alarms_bal_real}} with 28 days behind the long ones (first row; Section 6.5 adds each long width in turn to the two short ones with 7 days). The gate removes a width where its reading cannot mean anything, a window that usually holds a handful of rows, and this is where the thin tweets' false alarms are: {{alarms_nogate_real}} alarms on the real series without it, three of them on CRM, CVS and PFE, and {{alarms_bal_real}} with it. The typical window it tests must be the median of the same window on past days, not a sum of per-bucket medians: on the Apache error log, two hundred lines a day in a tenth of its buckets, the per-bucket median is zero at every time of day, the summed level is zero at every width, and the gate refuses the source. Under a summed level the Apache error log would be judged {{s:shipped_logs.json:Apache error log, 264 days:7:months}} of its {{s:shipped_logs.json:Apache error log, 264 days:2:months}} source-months and BGL {{s:shipped_logs.json:BGL supercomputer, 215 days:7:months}} of {{s:shipped_logs.json:BGL supercomputer, 215 days:2:months}}. The summed row is at one kind of day, since day groups need the typical window.

Day groups buy the weekly rhythm and the thin series' degradations, and cost the taxi two alarms. With one kind of day the real series raise {{alarms_onekind_real}} alarms, AAPL's and IBM's; day groups add {{s:shipped.json:NYC taxi:2:false_count}} on NYC taxi, where one kind raises none. In return, on NYC taxi drops to 30% caught within the hour go from {{s:shipped.json:NYC taxi:6:by60_0.3}} to {{s:shipped.json:NYC taxi:2:by60_0.3}} and halvings within the day from {{s:shipped.json:NYC taxi:6:by1440_0.5}} to {{s:shipped.json:NYC taxi:2:by1440_0.5}}; on Tweets FB and GOOG drops to 30% within the day go from {{s:shipped.json:Tweets FB:6:by1440_0.3}} and {{s:shipped.json:Tweets GOOG:6:by1440_0.3}} to {{s:shipped.json:Tweets FB:2:by1440_0.3}} and {{s:shipped.json:Tweets GOOG:2:by1440_0.3}}; and on the thin Tweets CRM, IBM and PFE from {{s:shipped.json:Tweets CRM:6:by1440_0.3}}, {{s:shipped.json:Tweets IBM:6:by1440_0.3}} and {{s:shipped.json:Tweets PFE:6:by1440_0.3}} to {{s:shipped.json:Tweets CRM:2:by1440_0.3}}, {{s:shipped.json:Tweets IBM:2:by1440_0.3}} and {{s:shipped.json:Tweets PFE:2:by1440_0.3}}. No series catches less within the day with day groups than with one kind; within the hour a few lose up to four points, the stops on Tweets CRM, IBM and FB among them. On the modeled weekday/weekend shape drops to a tenth within the hour go from {{s:shipped.json:weekday/weekend:6:by60_0.1}} to {{s:shipped.json:weekday/weekend:2:by60_0.1}} (Section 4.3).

The gate's bound is a constant, not a lever. Its value is set by what a quantile of ratios can mean with few rows: a window that holds a handful of rows takes a handful of distinct ratio values, and the lowest 0.05% of them is noise. Sweeping it at Balanced:

{{GATE}}

Ten rows is the smallest gate that removes the alarms of windows that hold a handful of rows, and the largest that keeps the 5-minute width on the mid-sized tweets; the modeled series, which fill every width, barely see the gate. At 5 rows the widths judge the thinnest tweets at a handful of rows a window: CRM and PFE each raise an alarm, and stops within 20 minutes rise from {{p:shipped_gate.json:nab:1:by20_0.0}} to {{p:shipped_gate.json:nab:0:by20_0.0}}. At 20 rows the 5-minute width is gated out on the tweets with fewer than 20 rows a bucket, FB, GOOG and KO among them, whose stops then wait for the 30-minute width, and the 30-minute width is gated out where it holds fewer than 20, which takes IBM's alarm with it: stops within 20 minutes fall to {{p:shipped_gate.json:nab:2:by20_0.0}} and within the hour from {{p:shipped_gate.json:nab:1:by60_0.0}} to {{p:shipped_gate.json:nab:2:by60_0.0}}. At 40 rows no further alarm goes and they fall to {{p:shipped_gate.json:nab:3:by20_0.0}} and {{p:shipped_gate.json:nab:3:by60_0.0}}.

## 6. Alternatives tried

Every choice in Section 2 was made against an alternative that was replayed and lost, and this section keeps the record. The early harness (Section 9) keeps the baseline's judging and learning rules: windows under an alarm on either side are not learned and the band is refreshed every reading.

| Section | Question | Harness |
|---|---|---|
| 6.1 | Which band: the twelve candidates that led to pooled ratios with a margin and a wait | early |
| 6.2 | The margin and the wait at one width | early |
| 6.3 | Speed: a CUSUM, and widths between 5 and 30 minutes | early |
| 6.4 | What is learned under an alarm, and when a window may be learned | early; then the detector as written |
| 6.5 | Which long width, and with how much history | the detector as written |
| 6.6 | Widths defined in rows rather than minutes | the detector as written |
| 6.7 | A typical level per kind of day, in the forms tried | early; then the detector as written at Balanced |
| 6.8 | The alternatives to the gap and to a value's own series | two of their own, on the modeled sender mixes |
| 6.9 | How long the gap remembers, and its margin | the gap alone, on modeled sources over years |

### 6.1 Pooling, margin and wait

Twelve bands at the 30-minute width, each keeping the baseline's judging and learning rules except that all but the first exclude an alarm's first window from learning.

| Candidate | Band | δ | k | Windows it rests on |
|---|---|---|---|---|
| Baseline | 99% of the same time of day's windows | 0 | 1 | about 91 |
| Baseline, first window not learned | as baseline | 0 | 1 | about 91 |
| Baseline + 10% | Baseline band, then 10% wider each side | 0.1 | 1 | about 91 |
| Pooled raw + 10% | 99.9% of every window of the week, raw | 0.1 | 1 | about 2,000 |
| Ratios | m(t) × 99.9% band of every window's ratio to its own time of day's typical level | 0 | 1 | about 2,000 |
| Ratios + 10% | Ratios, then 10% wider each side | 0.1 | 1 | about 2,000 |
| Negative-binomial + 10% | m(t) ± 99.9% band of every window's residual, scaled by a fitted variance | 0.1 | 1 | about 2,000 |
| Ratios + 10%, 3-day half-life | Ratios + 10%, each window weighted by 0.5^(age in days / 3) | 0.1 | 1 | about 2,000, fewer in effect |
| Ratios + 10%, day groups | Ratios + 10%, with the typical level learned per group of weekdays found by clustering | 0.1 | 1 | about 2,000 |
| Ratios + 20% | Ratios, then 20% wider each side | 0.2 | 1 | about 2,000 |
| Ratios + 10%, 3 in a row | Ratios + 10%, alarmed only on the third consecutive reading below; those readings not learned | 0.1 | 3 | about 2,000 |
| Ratios + 20%, 3 in a row | Ratios + 20%, alarmed only on the third consecutive reading below | 0.2 | 3 | about 2,000 |

Day groups follow the shape of Splunk's assisted thresholding (Rajyaguru, Bai and Curtis, 2023), which clusters one-day subsequences and keeps the clustering only when the median silhouette passes a cutoff. The choices here are ours: each of the last 28 days becomes 24 hourly sums, days are clustered into two or three groups, the grouping is kept if its median silhouette is at least 0.2, and each weekday joins the group most of its days fell in. Section 6.7 gives the forms that followed and the one that ships.

<?claude block chart1?>

False alarms per source-month, by series:

| Candidate | Steady | Variable | Daily | Thin daily | Weekday/weekend | Steady, falling 3%/day | Variable, falling 3%/day | Tweets (10) | NYC taxi | AWS (3) |
|---|---|---|---|---|---|---|---|---|---|---|
| Baseline | 146.8 | 137.2 | 136.7 | 102.5 | 88.8 | 491.6 | 305.6 | 132.3 | 93.5 | 205.8 |
| Baseline, first window not learned | 198.2 | 184.2 | 164.1 | 185.3 | 103.2 | 493.0 | 352.5 | 174.4 | 120.0 | 252.0 |
| Baseline + 10% | 10.7 | 98.6 | 58.9 | 91.4 | 77.5 | 198.3 | 204.1 | 118.1 | 7.1 | 61.6 |
| Pooled raw + 10% | 0.0 | 7.5 | 1.8 | 0.0 | 7.1 | 5.4 | 15.0 | 10.6 | 1.3 | 4.8 |
| Ratios | 31.8 | 20.4 | 18.2 | 10.0 | 11.8 | 349.5 | 108.3 | 29.5 | 14.7 | 14.4 |
| Ratios + 10% | 0.4 | 8.2 | 5.7 | 10.4 | 4.6 | 12.9 | 19.6 | 16.8 | 1.6 | 9.6 |
| Negative-binomial + 10% | 0.4 | 8.2 | 6.1 | 9.6 | 4.3 | 12.1 | 19.6 | 16.7 | 1.6 | 9.6 |
| Ratios + 10%, 3-day half-life | 0.4 | 7.9 | 5.0 | 8.9 | 4.3 | 7.9 | 13.6 | 14.1 | 1.1 | 9.6 |
| Ratios + 10%, day groups | 0.4 | 7.5 | 2.5 | 13.6 | 6.1 | 12.9 | 20.7 | 13.8 | 4.0 | n/a (14 days) |
| Ratios + 20% | 0.0 | 4.6 | 0.7 | 8.2 | 0.0 | 0.0 | 3.6 | 9.7 | 0.0 | 6.4 |
| Ratios + 10%, 3 in a row | 0.4 | 5.7 | 2.1 | 0.7 | 1.8 | 1.4 | 11.4 | 4.2 | 1.5 | 1.6 |
| Ratios + 20%, 3 in a row | 0.0 | 2.5 | 0.0 | 0.7 | 0.0 | 0.0 | 1.4 | 1.9 | 0.0 | 0.0 |

Drops detected within an hour, as a share of judged buckets:

| Candidate | Real: stops | Real: halvings | Modeled: stops | Modeled: to a tenth | Modeled: halvings |
|---|---|---|---|---|---|
| Baseline | 86% | 59% | 98% | 97% | 79% |
| Baseline, first window not learned | 87% | 64% | 98% | 97% | 81% |
| Baseline + 10% | 87% | 57% | 98% | 97% | 75% |
| Pooled raw + 10% | 78% | 15% | 86% | 75% | 42% |
| Ratios | 78% | 23% | 98% | 93% | 59% |
| Ratios + 10% | 79% | 15% | 98% | 91% | 49% |
| Negative-binomial + 10% | 84% | 14% | 97% | 90% | 50% |
| Ratios + 10%, 3-day half-life | 78% | 14% | 98% | 91% | 48% |
| Ratios + 10%, day groups | 81% | 22% | 98% | 97% | 52% |
| Ratios + 20% | 79% | 10% | 98% | 88% | 40% |
| Ratios + 10%, 3 in a row | 76% | 9% | 97% | 88% | 45% |
| Ratios + 20%, 3 in a row | 76% | 6% | 97% | 85% | 37% |

Read down the rows on the real series. The baseline raises 145 false alarms a month, 89 to 492 by series, on the archive's labels alone (131 in Section 4, with the two labels of Section 3 added), more than its 91 windows suggest, because the windows are dependent and the edge flickers. Leaving an alarm's first window out of its learning raises that to 187, 103 to 493 by series, since the baseline leans on those windows to stay wide. Widening it 10% brings the steady series to 11 and the taxi series to 7 and leaves the rest at 59 to 204. Pooling the week's raw windows is as quiet as ratios but blind at quiet hours: no stop is detected on the thin daily series and 42% of drops to a tenth on the weekday/weekend series. Ratios take 145 false alarms a month to 25. A 10% margin on them takes 25 to 14. Negative-binomial residuals match ratios within one alarm a month, and add a fitted variance. A three-day half-life trims false alarms by 14 to 23%, and adds a fitted weight. Day groups lift drops to a tenth on the weekday/weekend series from 63% to 100%, and on trending series the clustering sorts days by age, not weekday. A 20% margin takes 25 to 8. Three consecutive readings take 8 to 1.3, 0.7 on the modeled series.

The baseline's false alarms are edge flicker: the window sits a median of 5% under the lower edge, the alarm lasts a median of 20 minutes, half come within two hours of the previous one. With ratios + 20% and four readings (Section 9), the real series raise 8 alarms in about 15 source-months, all under 30 minutes, a median of 31% under the edge, half on one tweet series in its morning hours.

### 6.2 The margin and the wait at one width

Twenty settings of the ratio band at the 30-minute width: margins δ ∈ {0, 0.1, 0.2, 0.3, 0.4} and waits k ∈ {1, 2, 3, 4}. On real series false alarms range 150-fold, from 0.17 to 25 a month, while drops detected move from 45% to 64%.

<?claude block chart2?>

Stops barely trade: every setting detects 76% to 79% of stops on real series, and the stops missed are on series too sparse for a 30-minute window. The trade is in partial drops: from δ = 0.2, k = 4 to δ = 0, k = 1 multiplies false alarms 36-fold and lifts drops to 30% detected from 20% to 46%. Waiting is cheaper than widening: δ = 0.2, k = 4 raises 0.69 a month and detects 51% of drops; δ = 0.3, k = 3 raises 0.65 and detects 49%; every setting on the frontier under 5 false alarms a month has k ≥ 3.

### 6.3 Speed: a shorter width against a CUSUM

A stop must be detected fast. The baseline detects a stop after a median of 15 minutes on real series (90% within 30); the 30-minute width with a margin and a wait takes a median of 40 minutes (90% within 45): about 25 minutes for the window to empty past the wider band, then the readings waited.

Two routes to speed were replayed. The first, a rank-based CUSUM, cannot be both fast and quiet. Each 5-minute bucket scores −log₁₀ of the rank of its ratio among the past week's pooled ratios (Section 2.2), a floor κ is subtracted, the excess accumulates over a run of low buckets, and the run alarms at a bar H (Page, 1954, with ranks for residuals). A bucket can be no rarer than one in a week, about 3.3 decades, so the evidence one reading adds is bounded by 3.3 − κ, and the (κ, H) that is quiet on the modeled series needs eleven empty readings to alarm. At that (κ, H) pair it raised 1.06 false alarms a month averaged over the real series, 5.5 pooled, almost all on the taxi series, where correlated dips accumulate, and detected 54% of stops within the hour. A CUSUM on standardized residuals was noisier still: tweets and taxi rides have heavier tails than a Gaussian, and a dip lasts about 50 minutes, so its buckets are not independent evidence and one low reading is followed by several more.

The second route is the band itself at a shorter width, which is the design. The 5-minute width is quiet because its reading is below its band only when the count is near nothing, and fast because the k-th empty bucket comes at 5k minutes. Widths between 5 and 30 were replayed on the early harness (Section 9): at δ = 0.3, k = 3, adding 15 minutes, or 10 and 15, to the two widths raised 1.75 and 2.00 false alarms a month on the real series, means over series, against 1.16 for the two alone under that harness, and caught the same stops, 80% within the hour at all three.

### 6.4 Learning

On the ratio band at the 30-minute width at δ = 0.2, k = 4, on modeled step series that double, halve or stop on day 35; what is learned does not depend on the margin. Learning the windows under an above-normal alarm cuts above-normal alarms on real series from 3.4 to 1.3 per source-month, and a steady source that doubles is learned within an hour instead of 9 to 56 hours; on the daily shape a doubling takes 14 to 32 hours either way on two seeds and on the third is learned only as the replay ends, after 502 hours, and nothing below changes. Learning the windows under a below-normal alarm at a reduced weight fails: the band's edge is the rarest of 2,000 windows, so even a small weight on the new level moves it, and a source that halves is learned within 3 hours on the steady shape and 10 on the daily at weight 0.1 or 0.01, where without the weight the alarm stands up to 13 hours, until the readings that never reached k have taught the new level; a stopped source's alarm ends a day sooner at either weight, 96 hours against 121, because with no usable band the alarm can only end on a reading inside the band. An alarm at one width unlearns the buckets of its own readings only, w per reading, so a 5-minute alarm takes one bucket a reading out of learning, and with it the long windows that overlap that bucket.

When a window may be learned is the other half of the rule. A window learned as soon as it ends teaches a long width the first hours of a drop that takes the width to show, and the width's low follows the drop down. With the four widths at δ = 0.2, k = 4 and one kind of day, on the real and modeled series with their injected drops, against a cut that is never learned at all:

{{LEARN}}

Learned once w + k buckets have passed, the detector catches nearly what it would if the injected cut were never learned: halvings within the day {{p:extend_learn.json:nab:0:by1440_0.5}} on the real series against {{p:extend_learn.json:nab:3:by1440_0.5}}, where a window learned at once catches {{p:extend_learn.json:nab:2:by1440_0.5}} and one learned after k buckets {{p:extend_learn.json:nab:1:by1440_0.5}}. The wait costs false alarms on the real series, {{c:extend_learn.json:nab:0:false}} in {{mo:extend_learn.json:nab:0:false}} source-months against {{c:extend_learn.json:nab:2:false}} learned at once, all on the tweets, and the shipped detector pays it at every preset. On modeled steady and daily sources that halve, fall to 30% or stop on day 35 and stay, three seeds each, at δ = 0.3: with the w + k wait every halving is alarmed within {{steps_first_max}}, and with the k wait alone {{steps_wait_missed}} of the {{steps_halvings}} halvings are learned before they alarm. Of the {{steps_n}} stepped sources, {{steps_learned}} end their alarm because the new level is learned, after {{steps_learned_range}}, all on the daily shape, through readings below the edge that never reached k in a row (Section 2.5); the rest stay alarmed until no width has a band.

### 6.5 Which long width, and with how much history

A third width was added to the two short ones at δ = 0.3, k = 3 with 7 days of history, scored within the hour and at 6 hours and 1 day; every cell is real series / modeled series.

| Third width | False a month | To 30%, within 1 h | within 6 h | within 24 h | Halving, within 1 h | within 6 h | within 24 h |
|---|---|---|---|---|---|---|---|
| none | 0.49 / 0.20 | 21% / 53% | 36% / 73% | 52% / 85% | 5% / 26% | 15% / 46% | 33% / 65% |
| 1 hour | 1.61 / 0.20 | 22% / 54% | 47% / 79% | 68% / 90% | 5% / 26% | 19% / 51% | 39% / 73% |
| 3 hours | 1.41 / 0.20 | 21% / 53% | 58% / 84% | 77% / 91% | 5% / 26% | 30% / 56% | 51% / 80% |
| 6 hours | 0.74 / 0.20 | 21% / 53% | 58% / 85% | 78% / 91% | 5% / 26% | 30% / 58% | 52% / 85% |
| 1 day | 1.41 / 0.20 | 20% / 53% | 35% / 73% | 80% / 91% | 4% / 26% | 14% / 46% | 53% / 85% |

Within the hour nothing changes at any width: a halving does not pull a long sum below its band in an hour. Each width pays at its own horizon, and with 7 days behind it each pays in false alarms on the thin series, where a long window's lowest ratio of the week sits near zero: the 1-hour and 3-hour widths added 14 and 11 alarms on the thin tweet series, the 1-day width 5, two on the thin tweet series and three on the EC2 series whose bursts set its level. The 6-hour width was the cheapest, 3 alarms, and bought the most within its horizon. The design keeps 6 hours and 1 day and gives both 28 days of history, which is what removes those alarms (Section 5.4).

### 6.6 Widths defined in rows

Instead of fixed widths with a gate, the widths were defined from the source's rate: a ladder of rungs from 5 minutes to a day, the fast width the shortest rung whose typical window holds B rows, the slow width the shortest holding 6B, picked afresh at each reading, with 7 days of history behind every rung. At B = 10 it raised 26 false alarms on the real series against 6 for the two fixed short widths at δ = 0.3, k = 3, the setting of this experiment, all 26 on four of the thin tweet series, where a rung of hours or a day was chosen and its 7-day quantile was the worst of seven days. Holding the two short widths as a floor and lengthening only from there changed nothing, 26 alarms in the same places. At B = 20 the alarms fell to 5 but the thin series' stops caught within the hour fell with them, to 56% on IBM, 48% on CRM and 18% on UPS, since the fast width had moved to an hour or more. The ladder's failure was the history, not the idea: fixed widths with the gate and 28 days behind the long ones (Section 5.4) judge a thin source at the widths it fills, which is what the ladder was for, with one rule instead of a chooser.

### 6.7 Day groups

The one modeled series on which pooled ratios miss drops to a tenth is the weekday/weekend shape, because a weekend's windows sit at a fifth of a weekday's inside the same pool and the edge sits too low for a weekday. Day groups (Section 2.1) are the remedy that ships. Four forms were replayed before it, the first of them twice, each against a rule fixed before its run, and two ways of judging a long window that reaches into a day of another kind.

The first form was put into the early detector at δ = 0.3, k = 3 with the two short widths: each of the last 28 days becomes 24 hourly sums, days are clustered into two or three groups and the grouping is kept when its median silhouette reaches 0.2, each weekday joins the group most of its days fell in, and the typical level is the median over the learned buckets of the same group within 30 minutes of the time of day over 28 days.

| Series | False a month, plain | grouped | Drops to a tenth in 1 h, plain | grouped | Drops to 30% in 1 h, plain | grouped | Silhouette |
|---|---|---|---|---|---|---|---|
| Weekday/weekend (modeled) | 0.00 | 0.00 | 34% | 100% | 17% | 54% | 0.79 |
| NYC taxi | 0.00 | 0.18 | 95% | 99% | 18% | 69% | 0.70 |
| Tweets FB | 0.00 | 0.00 | 97% | 99% | 23% | 50% | 0.36 |
| Tweets GOOG | 0.00 | 0.00 | 95% | 99% | 19% | 39% | 0.42 |
| Tweets AMZN | 0.00 | 0.00 | 100% | 100% | 83% | 85% | 0.46 |
| Tweets AAPL | 0.00 | 0.00 | 89% | 87% | 34% | 35% | 0.34 |
| Tweets KO | 0.00 | 0.00 | 85% | 82% | 15% | 15% | 0.16 |
| Tweets IBM | 2.4 | 1.2 | 12% | 29% | 2% | 1% | 0.35 |
| Tweets CRM | 1.1 | 3.4 | 20% | 32% | 3% | 3% | 0.38 |
| Tweets UPS | 3.3 | 3.3 | 6% | 8% | 2% | 1% | 0.05 |
| Tweets PFE | 0.00 | 1.1 | 0% | 1% | 0% | 0% | 0.34 |
| Daily, steady falling (modeled) | 0.00 | 0.00 | 100% | 100% | unchanged | unchanged | 0.0, 0.60 |

Where the week has a shape the groups find it and the band tightens: every drop to a tenth on the weekday/weekend shape, and 3.8 times the drops to 30% within the hour on the taxi series and about twice on FB and GOOG, which have a weekly rhythm of their own. The cost falls on the thin tweet series, where the silhouette passes on noise at 0.34 to 0.38 and the grouped level is estimated from a third of the days: 3 more false alarms in 14 source-months, 2 of them on the thin tweets. A silhouette measures how well the days it was fitted on separate, not how well the grouping predicts days it was not, and every later form is tested on the second: a grouping is kept only when, with each of four weeks held out in turn and the grouping fitted on the other three, it predicts every held-out week better than one kind does, which luck passes at most one time in sixteen. The candidates are fixed at one kind, two kinds and seven, one per weekday.

Four forms were then replayed at Balanced with the four widths, days split at UTC midnight: A clusters the days as above; B takes the best of the 63 ways to split the weekdays in two; C is B with each day starting at the source's quietest hour; H takes a split of the weekdays per hour of the day. Real series are the 14 count series and the 10 logs; held-out error removed is the share of a held-out week's error the kept grouping removes against one kind, over the 27 series long enough for the test, the seven modeled shapes, the ten tweet series, NYC taxi and nine of the logs, negative where a grouping predicts worse:

| | One kind | A | B | C | H |
|---|---|---|---|---|---|
| Modeled: drops to a tenth within the hour | 79.0% | 88.4% | 88.4% | 88.4% | 88.4% |
| Count series: drops to 30% within the hour | 15.3% | 23.0% | 23.8% | 25.0% | 23.1% |
| NYC taxi: drops to 30% within the hour | 16.9% | 96.3% | 96.4% | 97.3% | 94.4% |
| Tweets FB: drops to 30% within the hour | 18.8% | 32.4% | 39.0% | 47.7% | 34.4% |
| Real series: false alarms, 54 source-months | 280 | 281 | 284 | 286 | 289 |
| Held-out error removed, mean of 27 series | | 12.8% | 12.3% | 13.1% | 7.2% |

The forms find the same weeks and differ in where a day starts and in how many splits they choose among. C catches the most: the tweets are counted in UTC, and their quietest hour, 4 to 8 UTC, is about midnight US Eastern, so C's days are the authors' days. H chooses a split 24 times a day and pays for it on the weeks it was not fitted on, worse than C on 24 of the 27 series and 16 to 18% worse than one kind where there is no weekly rhythm; on a modeled office week, whose weekly change lies in its working hours, the case H was built for, it catches within 3 points of C on short drops and fewer of the slow ones. The false-alarm bar set before the run, A's count plus one, passed A alone; the forms' counts differ by 1 to 8 of about 280, on the event logs, where the band raises about 200 with or without groups, and on one Saskatchewan alarm that A misses because A's own band is wider: with UTC days A judges Sunday-afternoon windows as Monday's, and those readings, at about half the usual, become the lowest of its history. Of two kinds and seven, seven is never the worse: on the modeled weekday/weekend shape with UTC days, Friday and Sunday evenings fall in the wrong one of two kinds, the test keeps seven, and seven catches 46.7% of drops to 30% within the hour against 36.4% for two forced. On the modeled office week grouping takes drops to 30% within the hour from 18% to 51% and halvings within the day from 31% to 55%; on a nightlife week whose weekend runs two hours later the test keeps one kind at most refreshes and grouping gains 5 points, as little as the rhythm offers.

With two kinds, a long window that reaches into the day before is not of one kind: Monday's 1-day window holds Sunday. Set against windows that do not cross a boundary of kinds, its typical level is wrong. The first remedy split each such window's typical level into the part from the day before, learned from that day's kind, and the part from today, learned from today's; it lifted halvings caught within the day on the office week from 54% to 92% and on the real series that ever kept a grouping by 4.4 points. Without seven kinds to fall back on, the split costs NYC taxi its drops to 30%, 97% caught within the hour against 10%, because two kinds fit the taxi worse than one for short drops, so seven stays a candidate. What ships replaces the made-up windows of the split with real ones: a window's typical level is the median of the same window on the newest 7 days of its day's kind, and a window that reaches into the day before uses only days whose own day before was of the same kind. On the 24 real series it catches within 0.3 points of the split, with 292 false alarms against 291, and a source of one kind is judged by the daily rule unchanged. Section 5.4 gives what the shipped day groups buy and cost at Balanced.

### 6.8 Sender rules

Two sender-specific rules were replayed and set aside. A gap judged per time of day, alarming when a value has been quiet longer than it ever was at this time of day in the past week, detects a steady sender's stop in 10 minutes with no false alarm and raises 455 to 1,750 false alarms per source-month on the modeled source of a hundred senders at a row a minute each, 5 to 17 per sender; the gap of Section 2.8 keeps the idea, drops the time of day, which left each hour a handful of spells to beat, and adds the margin and a year of history. A share band, each value's share of the source's rows against a pooled band of shares, detects every stop within the hour at a median of 40 minutes with δ = 0.4, k = 4, misses halvings, and sees nothing the value's own series does not.

### 6.9 How long the gap remembers

Three other memories and one other margin were replayed on modeled sources that fire about every two weeks. Twenty-eight days of spells with a gate of ten, the count widths' history, judges a source that fires in bursts every two weeks only right after a burst, when the burst's short spells have filled the gate, and then on a record of two spells: {{rare_false_28}} false alarms a year (Section 4.7). The last 30 spells, however old, judges such a source nearly all the time and is undone by bursts: one burst fills the 30 with short spells, the long ones are forgotten, and the next normal silence beats a record of minutes. On a source that fires every 15 days at random, three events in ten a burst, it raised {{gh_last30}} false alarms in {{gh_years}} source-years where 28 days raised {{gh_28}}. A spell remembered for 28 days or 30 times its own length, whichever is longer, is not undone by bursts and raised {{gh_own30}}, but its 30 is a constant with no reason behind it, where a year is the false-alarm period. At margin 1 a year failed: on the source that fires in bursts every two weeks it raised {{c1_false}} false alarms a year after its first year, because a spell that alarmed is not learned, so the record can only fall as old spells leave the year and every spell over it alarms again. At 1.5 it raised {{rare_false_year}}.

## 7. Discussion and limitations

The story of the introduction rests on one fact, on three levers, on a gate that follows the source's rows, and on judging in two coordinates, rows per unit of time and time per row. This section says what each of those rests on in turn, and where the ground is thin.

*What the fact buys.* Every band edge is an empirical quantile, so the per-reading false-alarm rate under exchangeability is known in advance and no tail is assumed. The gap's year is set by the same fact, as the history whose length is the false-alarm period, and its first year's allowance by the law of spells that end at a steady chance; the predictions for the year were written before it was replayed, and it met them once its margin was over 1 (Section 6.9). The constants beside the edges, the gate's ten rows and the margins δ and m, were set by the replay. What the fact does not cover, dependence between windows and between readings, is paid for empirically by the margin and the wait, and the replay is the only measure of what they cost. The margin and wait were chosen on the real count series; the real logs were scored after the band was fixed, and their false-alarm rates (Section 4.2) are the out-of-sample measure this paper has for the band. The gap's margin was chosen with the logs in view (Section 4.7), so the logs are not out of sample for it.

*Real logs are of two kinds, and the two coordinates divide them.* Four of the ten logs have lines in nearly every bucket, and on them the count widths do what they do on the count series. The other six, an error log, a syslog, an authentication log, a supercomputer's events, a coordination service and a university web server at three lines a bucket, are judged by the count widths at the long widths or not at all, and where those judge they alarm, because a count of events that happen when something goes wrong has no normal rate; the same logs have a normal silence, and the gap judges them by it: {{q:Apache error log, 264 days:1:false}}, {{q:Linux syslog, 264 days:1:false}} and {{q:BGL supercomputer, 215 days:1:false}} alarms a month on the Apache error log, the Linux syslog and BGL where the count widths raise {{q_apache_band}}, {{q_linux_band}} and {{q_bgl_band}} (Sections 4.2 and 4.7). The kinds describe the logs; the detector applies one rule, every width on every source where its gate admits it, and the operator sees one message. Section 4.8 scores the five widths together on every source.

*The count widths are one estimator, and a fast one.* The four count readings use the same typical window, the same ratio, the same quantile and the same wait; a width differs from the next only in what it sums. Their edge shares a bound with a rank-based CUSUM over the same pooled ratios: a reading below the design's edge, near the third-lowest of 2,000 ratios, is at most about 2.8 decades rare, and below the lowest 3.3, for either. The band is faster for two reasons. The margin is a value criterion past the extreme, so a reading far below the edge counts for more than the rarest rank; and the wait has no floor, so k readings merely below alarm, where the CUSUM subtracts a floor κ from every reading's evidence and at its quiet (κ, H) pair needs eleven empty readings to reach its bar.

*A quantile needs windows that do not overlap.* A week holds about 2,000 distinct half-hour windows and 7 distinct days. The 0.05% quantile of a 1-day width over a week is the worst day of the week, and a source 20% under its worst day is an alarm; over 28 days it is the worst of 28. The history behind a width should grow with the width; of the two histories tried, 7 and 28 days, 28 is the one that made the 1-day width quiet (Section 5.4). A width of a week would want months.

*Halvings.* No setting of this band catches most halvings within the hour on a variable series without the baseline's alarm rate (Sections 6.1 and 6.2), and no width does (Section 6.5): a halving does not empty a long window in an hour. The 6-hour and 1-day widths catch {{by360_05_bal_real}} of halvings within 6 hours and {{by1440_05_bal_real}} within the day on the real series, and that is the route to a halving this work found.

*Bursty series.* On one EC2 series a fifth of the 30-minute windows are bursts over ten times the median window (Section 9), at roughly repeating times of day. The detector as written learns the windows under an above-normal alarm, and catches {{s:shipped.json:EC2 network, 5abac7:2:by60_0.0}} of the stops on that series, EC2 network 5abac7 of Section 4.1, within the hour and {{s:shipped.json:EC2 network, 5abac7:2:by1440_0.0}} within the day; the early harness, which leaves those windows out, takes a burst as the typical level of its time of day and catches 27%. The BGL log is a burstier case still, an empty median bucket and bursts of up to 42,000 lines, and the band raises {{q_bgl_band}} false alarms a month on it, the most of any log: a series made of bursts has no level for a ratio to be taken against. The gap, which asks only how long the quiet between bursts has run, raises {{q:BGL supercomputer, 215 days:1:false}} a month there.

*Weekly patterns.* Day groups catch every drop to a tenth on the weekday/weekend shape and take NYC taxi's drops to 30% within the hour from {{s:shipped.json:NYC taxi:6:by60_0.3}} to {{s:shipped.json:NYC taxi:2:by60_0.3}}, for two false alarms on the taxi (Sections 5.4 and 6.7). They find a week and nothing longer. The band's alarms on the logs come mostly from the three sporadic system logs, which the gap judges better (Section 4.7); on the university web logs, which have an academic rhythm of terms and holidays beside the week, it raises {{q_calgary_alarms}} alarms on Calgary and {{q_sask_alarms}} on Saskatchewan over their records, against {{s:shipped_logs.json:Calgary web, 1994-95:6:false_count}} and {{s:shipped_logs.json:Saskatchewan web, Jun-Dec 1995:6:false_count}} with one kind of day. A rhythm longer than a week is what remains there, and the 28 days behind the long widths do not hold a term.

*Above normal* is judged at 30 minutes only, the one width the replay scored it at.

*Data.* The count series and logs are public, not the application's own, and the sender mixes are modeled. A replay of a deployment's own 5-minute counts, per source and per sender, is the check these numbers need; the harness takes a timestamp,count CSV.

## 8. Application

In smeltr the five widths live in `src/leader/throughput.rs`, and the kinds of day in `throughput/week.rs`. The leader judges every source from the 5-minute buckets it already keeps, at the preset an editor picked for it, Balanced until one does, and each watched sender at the preset picked for its source's senders, Quiet until one does. A pick applies from the next reading to everything already learned, and it is one of the four points measured here, never a value between them. The leader has its own words for the paper's: the band is a source's range, the typical window its usual window, a spell a quiet spell, an alarm a warning, and an operator an editor. The four count widths replace the baseline, the long widths learn from 28 days of buckets where it learned from 7, and the chart draws the band that is judged. The gap is a second statistic beside them, with a store of its own: each source's quiet spells of the past year, recorded each hour, since the buckets are kept only four weeks. The normal is learned again each hour and a source's kinds of day each day; every width judges every source where its gate admits it, a stop is reported by whichever fires first, and a source is judged once its rows reach back a week.

Either coordinate raises the same warning, Rows below normal. It holds until some width judges the source and none reads it below its range, and where no count width judges the source it ends at the first row that ends a quiet spell the gap read below. Days not learned are passed over, not counted, so a held warning is judged against the days before it until those are no longer kept; after that no width judges the source and it still warns, so a source that has gone quiet stays reported.

What is learned is one set for every width: the time the source ran, less the time under a below-normal warning or a stop, plus the time of any warning an editor marked Normal. The replay keeps the two coordinates' histories apart (Section 2.5); in the leader a warning from either keeps both from learning. An editor marks a below-normal warning Normal or Outage. Outage marks a real outage: the warning turns into information, stays on the Issues page until it ends, and is not learned. Normal turns it into information and is learned whole, by the count widths and the gap alike and without the learning wait of Section 2.5, so marking the warning that is up Normal teaches the level the source reads now and ends the warning within the hour. A warning nobody marked stays a warning and is not learned. Reset history forgets what the source read before now, so it has no normal until its rows reach back a week again. The marks are how a source that changes its habit is taught it: a silence that warned is never learned, so a source that now goes quiet for twice its old record warns every time until an editor marks one such warning Normal. The source's History tab lists its warnings of the past year and its longest quiet spells, each shown Normal when it is learned and Outage when it is not. A sender's warning is marked as a source's is, and the senders of a source learn from the time a source warning marked Normal was up.

The cost is small. A count reading builds a typical window and about 2,000 to 8,000 ratios and their quantile per width, about 200,000 floating-point operations against about 90 for the one band, a few milliseconds per source in Python and well under a second per 5-minute pass for 250 sources in Rust. The kinds of day are learned once a day from four weeks of hourly sums. The gap adds a year of silences per source, up to about eight thousand for a source with a row an hour, every one kept so that a mark can change which is the longest, and one comparison per reading.

For senders, workers count each value of every string field the pipeline delivers, and of the source's stream key, by the rows that held it, on every batch; a field that holds more than 256 values on a worker names no sender and is counted no more. The leader sums each 5-minute bucket's counts and writes it, once the bucket ends, into a day row for each field and each value: the buckets the field was counted in, the buckets the value held a row in, and for a watched sender the rows it held in each. Each day it picks the watched values by the rule of Section 2.6, counting only the time the field was counted. A watched sender keeps its rows for the four weeks its normal is learned from, is judged once it has a week of them, and is not judged while its source warns that its rows are below normal or that it is stopped; senders of one source that cross on one reading raise one warning that names them.

## 9. Reproducibility

The harness is Python 3 with NumPy, in `docs/research/normal-detector/lab/` beside this paper. Every run is seeded and was reproduced row for row; the fetched series and the runs' outputs are made by the commands below, and each comment names the sections the run feeds.

```
python3 fetch_nab.py nab/              # the 14 NAB series, their labeled windows and license
sh fetch_logs.sh ../fetch
    # nine of the ten raw logs, unpacked under ../fetch/loghub/<log>/ and ../fetch/ita/
python3 real_logs.py ../fetch real/
    # 5-minute counts and fields from the ten logs (Sections 3 and 4.2)
python3 facts.py nab/ real/
    # the EC2 burst share and Apache's longest silences (Sections 4.2 and 7)
python3 split_run.py nab shipped shipped.json
    # the four presets and the parts removed one at a time, real and modeled series
    # (Sections 2.7, 4.1, 4.3 and 5.4)
python3 split_run.py logs shipped shipped_logs.json
    # the same on the ten logs (Section 4.2)
SHIPPED=1 python3 sparse_lab.py shipped_sparse.json          # sparse sources (Section 4.4)
SHIPPED=1 python3 sparse_lab.py shipped_sparse_history.json history
    # 7 and 28 days behind every width, and a week-wide width (Section 4.4)
python3 quiet_lab.py nab/ quiet.json
    # the gap at margins 1, 1.5 and 2 (Sections 2.8 and 4.7)
QUIET28=1 python3 quiet_lab.py nab/ quiet_28.json
    # the same with 28 days of spells and a ten-spell gate (Section 4.7)
python3 record_lab.py record.json
    # 28 days of spells against a year, at margins 1 and 1.5, on modeled sources over years
    # (Sections 4.7 and 6.9); its predictions, written first, are in record_rule_predictions.md
python3 gap_history_lab.py gap_history.json
    # 28 days, the last 30 spells, and 28 days or 30 times a spell's length (Section 6.9)
SHIPPED=1 python3 either_lab.py nab shipped_either.json
    # count widths and gap scored together (Section 4.8)
python3 split_run.py nab shipped_hist7 shipped_hist7.json
    # 7 days of history behind every width (Sections 4.4 and 5.4)
python3 split_run.py nab shipped_gate shipped_gate.json
    # the gate at 5, 10, 20 and 40 rows (Section 5.4)
SHIPPED=1 python3 extend_lab.py senders shipped_senders shipped_senders.json
    # each sender of each mix at Quiet and Balanced, every row counted (Section 4.5)
SHIPPED=1 python3 values_logs.py real/ shipped_values_logs.json
    # the sender criterion and each watched value on the logs (Section 4.5)
SHIPPED=1 python3 discover_lab.py discover.json
    # which modeled fields behave like senders (Section 4.5)
python3 split_run.py nab shipped_widths shipped_widths.json  # each width alone (Section 4.6)
python3 split_run.py nab shipped_grid shipped_grid.json
    # margins x waits at the shipped detector (Sections 5.2 and 5.3)
python3 heldout.py shipped_grid.json
    # pick on one half, score on the other (Section 5.2)
python3 baseline_drops.py nab/ baseline_drops.csv
    # the baseline on every drop fraction (Sections 1 and 4)
python3 run_all.py nab/ final.csv && python3 aggregate.py final.csv
    # the twelve bands (Section 6.1)
python3 sweep.py nab/ sweep.csv        # 5 margins x 4 waits at one width (Section 6.2)
python3 patterns.py nab/ patterns.json
    # minutes from a stop to its alarm; what false alarms and misses look like (Section 6.1)
python3 rank_lab.py nab/ rank.json     # the rank-based CUSUM, 16 pairs of floor and bar (Section 6.3)
python3 unified_lab.py nab/ unified.json  # widths between 5 and 30 minutes (Section 6.3)
python3 learn.py nab/ learn.json       # learning alarmed windows (Section 6.4)
python3 extend_lab.py nab/ learn extend_learn.json &&
    python3 extend_lab.py steps steps extend_steps.json
    # when a window may be learned, and stepped modeled sources (Section 6.4)
python3 extend_lab.py nab/ long extend_long.json
    # a third width with 7 days of history (Section 6.5)
python3 extend_lab.py nab/ rows extend_rows.json &&
    python3 extend_lab.py nab/ rowsfloor extend_rowsfloor.json
    # widths defined in rows (Section 6.6)
python3 extend_lab.py nab/ groups extend_groups.json
    # the first form of day groups, on the early detector (Section 6.7)
python3 extend_lab.py nab/ daygroups_forms extend_daygroups_forms.json &&
    python3 extend_lab.py logs daygroups_forms extend_daygroups_forms_logs.json &&
    python3 heldout_forms.py
    # forms A, B, C and H, and how well each predicts held-out weeks (Section 6.7)
python3 weekly_forms.py                # the office and nightlife weeks (Section 6.7)
python3 split_usual.py && python3 one_path.py
    # long windows that reach into a day of another kind (Section 6.7)
python3 share_lab.py share.json && python3 silence_lab.py silence.json
    # the share band and the time-of-day gap (Section 6.8)
python3 build_paper.py . paper_v7_template.md paper.md
    # every table and quoted number in this paper, from the files above
```

`design_lab.py` is the two-width detector as one class; `extend_lab.py` adds to it, one rule at a time, the long widths, the history per width, the gate, the typical window, the day groups, the learning wait, the week's wait and the rows-defined widths. Its `SHIPPED` is the detector smeltr ships, `PRESETS` its four presets and `AT` the detector at each; the `shipped` experiments are those beside the parts removed one at a time, and `SHIPPED=1` sets the sparse, either, sender and field runs to the same detector and to every sender row counted. `split_run.py` runs one series and config per process, so the longest series finish a config at a time; `run_chain.sh` runs every `shipped` step and resumes where it stopped. `normal_lab.py` is the early harness: it ports the baseline and holds every candidate band and the scoring; `run_all.py` the modeled series, the NAB loader and the two labels added after the replay found them (Section 3); `real_logs.py` the log parsers. The tables of Sections 6.1 and 6.5 to 6.8 and the numbers quoted beside them are copied into the template by hand from the outputs of the runs named for them (`aggregate.py` on `final.csv`, `extend_long.json`, `extend_rows.json`, `extend_rowsfloor.json`, `extend_groups.json`, `weekly_forms.json`, `split_usual.json`, `one_path.json`, `extend_daygroups_forms.json`, `heldout_forms.json`, `patterns.json`, `unified.json`, `share.json`, `silence.json`); every other table and quoted number is written by `build_paper.py` from the files above, and the template names the file and field each comes from. The Google cluster 2011 job-events tables are Google's own distribution, placed under `../fetch/gcluster/`. `md2tex.py` sets the paper in LaTeX, and `gen_diagram.py` and `diagram_code.js` draw Figure 1; `confidence_lab.py`, `five_lab.py`, `multi_lab.py`, `tail_lab.py` and `value_lab.py` are earlier experiments on the early harness, kept for the record, whose outputs the paper does not quote. A real source exported as a timestamp,count CSV joins the replay as one more series, and with no labeled windows every alarm on it counts as false.

## Appendix A. The distribution-free bound and the three levers

**Proposition 1.** Let X_1, …, X_{n+1} be exchangeable real random variables with no ties almost surely. Then

```latex
P\left(X_{n+1} < \min(X_1, \dots, X_n)\right) = \frac{1}{n+1}.
```

*Proof.* The event is that X_{n+1} is the minimum of the n+1 variables. By exchangeability the joint law is invariant under permutations of the indices, so each index is the minimum with the same probability, and with no ties exactly one of them is. The n+1 probabilities sum to one. ∎

**Corollary 2.** Let X_{(1)} ≤ … ≤ X_{(n)} be the order statistics of X_1, …, X_n (David and Nagaraja, 2003) and let r ∈ {1, …, n}. Then P(X_{n+1} < X_{(r)}) = r/(n+1). With ties, X_{n+1} < X_{(r)} implies that X_{n+1} has rank at most r under any tie-breaking, so P(X_{n+1} < X_{(r)}) ≤ r/(n+1); counts tie, and the inequality is the form used. A band whose lower edge is interpolated between X_{(r)} and X_{(r+1)} is undercut by an exchangeable next window with probability at most (r+1)/(n+1).

*Proof.* Without ties, X_{n+1} < X_{(r)} if and only if X_{n+1} has rank at most r among the n+1, and by exchangeability each rank is equally likely. ∎

**Corollary 3.** Let a source make spells at a rate of λ per unit of time, exchangeable among themselves, and let a spell set a record when it outlasts every spell that completed in the history of length T before it. Then records are set at a rate of at most about λ/(λT + 1), under 1/T, whatever the law of the spells: a spell has about λT spells in its history and by Proposition 1 outlasts all of them with probability at most 1/(λT + 1). Where spells bunch, as in bursts, the short spells of a burst never set a record and the bound applies to the long spells alone, which are fewer in a history and each likelier to set a record, but still set one at most about once per T. If the spells end at a steady chance, exponential with mean μ, two more facts hold. The longest of n has mean μ(1 + 1/2 + … + 1/n), about μ(ln n + 0.58), so it grows by μ each time n grows e-fold; and E[s²] / 2E[s] = μ, so μ̂ = Σs² / 2Σs estimates μ. The chance that the next spell outlasts m times the longest of n is

```latex
P\left(S_{n+1} > m \max_{k \le n} S_k\right) = \prod_{k=1}^{n} \frac{k}{k+m} \approx \Gamma(1+m)\, n^{-m},
```

about 1.33/(n√n) at m = 1.5.

*Proof.* The maximum of n exponentials with mean μ is distributed as μ(E_1 + E_2/2 + … + E_n/n) with E_k independent standard exponentials, which gives its mean, and P(S_{n+1} > mM) = E[exp(−mM/μ)] = ∏ E[exp(−mE_k/k)] = ∏ k/(k + m). ∎

This is the fact behind conformal prediction (Vovk, Gammerman and Shafer, 2005). It makes no assumption on the law of the windows, so the heavy tails of real count series cost nothing. It does assume exchangeability, which fails in two ways for windows of a count series.

*Time of day.* Windows at 3 a.m. and 3 p.m. are not drawn the same way on any series with a daily pattern. Pooling them raw destroys the bound's premise and, worse, destroys detection: the band's low edge is set by the quietest hour, and a daytime drop to the night level is inside it. The remedy is to divide each window by its time of day's typical level before pooling (Section 2.2).

*Overlap and drift.* Consecutive windows of width w share w−1 buckets, and the noise of a real series drifts over tens of minutes. The windows of a week are therefore far from independent, the effective n is much smaller than the count of windows, and consecutive readings are dependent: a window that sits at the band's edge produces a run of readings in and out of the band. Proposition 1 bounds the rate of readings below the band; it does not bound the rate of alarms raised, which under flicker is several per excursion. The wider the window, the fewer windows of a given history do not overlap: a week holds 7 windows of a day, and the bound's n for that width is nearer 7 than 2,000, which is why the long widths take 28 days (Section 2.2).

Three levers follow. More exchangeable windows lower r/(n+1) at a fixed rank: an edge at the lowest of 91 windows is undercut once in 92 readings; the design's edge, interpolated between the second- and third-lowest of about 2,000, at most 3 times in 2,017, once in about 670, a factor of seven. A width judged every five minutes makes 8,640 readings a month, so even that edge is undercut about 13 times a month under exchangeability alone, and a quantile by itself cannot reach one alarm a month. A margin below X_{(r)} requires the next window to fall further than any past one did; the fact does not price it, and what it buys depends on the tails of the series. A wait of k readings alarms only on the k-th consecutive reading below the band, so an excursion pays once, and a flicker shorter than k readings pays nothing. The detector pulls all three (Section 2); what each buys is measured in Section 6.1.

## References

- Arlitt, M. F. and Williamson, C. L. (1996). Web server workload characterization: the search for invariants. *Proceedings of ACM SIGMETRICS 1996*, 126–137. https://researchr.org/publication/ArlittW96. The logs are in the Internet Traffic Archive, https://ita.ee.lbl.gov/html/traces.html
- David, H. A. and Nagaraja, H. N. (2003). *Order Statistics*, 3rd ed. Wiley. https://doi.org/10.1002/0471722162
- He, S., Zhu, J., He, P. and Lyu, M. R. (2020). Loghub: a large collection of system log datasets towards automated log analytics. arXiv:2008.06448. https://arxiv.org/abs/2008.06448. The logs are at https://github.com/logpai/loghub
- Lavin, A. and Ahmad, S. (2015). Evaluating real-time anomaly detection algorithms: the Numenta Anomaly Benchmark. *IEEE ICMLA*. https://arxiv.org/abs/1510.03336
- Page, E. S. (1954). Continuous inspection schemes. *Biometrika* 41, 100–115. https://doi.org/10.1093/biomet/41.1-2.100
- Rajyaguru, O., Bai, H. and Curtis, K. (2023). ML-powered assistance for adaptive thresholding in ITSI. Splunk blog, 9 August 2023. https://www.splunk.com/en_us/blog/it/ml-powered-assistance-for-adaptive-thresholding-in-itsi.html
- Reiss, C., Wilkes, J. and Hellerstein, J. L. (2011). Google cluster-usage traces: format + schema. Google Inc. technical report. https://github.com/google/cluster-data
- Vovk, V., Gammerman, A. and Shafer, G. (2005). *Algorithmic Learning in a Random World*. Springer. https://doi.org/10.1007/b106715
