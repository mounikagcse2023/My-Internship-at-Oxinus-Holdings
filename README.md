# Data Engineering Internship — Baraka Social Benefits Platform

**Oxinus Holdings, Abu Dhabi · 23 June 2026 – 20 October 2026**
**Mounika G · B.E. Computer Science and Engineering - 2027**



---

## Contents

1. [The project](#1-the-project)
2. [My role and tools](#2-my-role-and-tools)
3. [Summary of my work](#3-summary-of-my-work)
4. [Data-quality monitoring](#4-data-quality-monitoring)
5. [P2P transfer pipelines](#5-p2p-transfer-pipelines)
6. [NASP disbursement analytics](#6-nasp-disbursement-analytics)
7. [Card transaction and balance analytics](#7-card-transaction-and-balance-analytics)
8. [P2P commission dashboard](#8-p2p-commission-dashboard)
9. [Dashboard quality assurance](#9-dashboard-quality-assurance)
10. [Reporting, documentation and handover](#10-reporting-documentation-and-handover)
11. [Timeline](#11-timeline)
12. [Engineering lessons](#12-engineering-lessons)
13. [Skills gained](#13-skills-gained)

---

## 1. The project

**Baraka** is the official social-benefits card and mobile application that Oxinus Holdings
built for Uzbekistan's **National Agency for Social Protection (NASP)**. Eligible citizens
receive state benefits on a Baraka Visa card issued by one of two partner banks, **Xalq Bank**
and **Aloqabank**. They then use the same card and app for purchases, cash withdrawals, mobile
payments and card-to-card (P2P) transfers.

Every disbursement, transaction and registration produces data. NASP, the partner banks and
Oxinus management need that data to answer questions such as:

- How much was disbursed this month, through which bank, and for which benefit?
- How do beneficiaries use their money once it arrives?
- Are the numbers in the reports complete and up to date?

The Data Engineering team runs the analytics platform that answers these questions. Data moves
from the production PostgreSQL systems through **Apache Airflow** pipelines into a warehouse
organised in **bronze, silver and gold layers**. It is checked by automated **data contracts**
and presented in **Metabase** dashboards and the **Weekly Business Report** inside the Baraka
web application.

## 2. My role and tools

I joined the Data Engineering team as an intern. I owned my components from design to
deployment, and every change went through a GitLab merge request reviewed by the team lead.

| Area | Tools |
|---|---|
| Warehouse and SQL | PostgreSQL: CTEs, window functions, stored procedures, materialized views, indexes, query plans |
| Orchestration | Apache Airflow: TaskFlow API, dynamic task mapping, trigger rules, retries, Variables |
| Business intelligence | Metabase: native questions, field filters, drill-downs, dashboards |
| Development | Python, VS Code, Git |
| Code review | GitLab merge requests |
| Issue tracking and QA | Oxigen (project-management and issue-tracking platform) |
| Alerting | Telegram Bot API, Slack |

## 3. Summary of my work

| # | Work | Type | Status |
|---|---|---|---|
| 1 | Data-contract framework: rules table and daily check pipeline | Airflow DAG + rules table | Deployed, daily at 08:00 |
| 2 | Baraka health-alerts Telegram bot | Alerting | In use by the data-contract check |
| 3 | P2P channel gold pipeline and its rebuild procedure | Airflow DAG + stored procedure | Deployed, daily at 07:00 |
| 4 | P2P daily summary pipeline | Airflow DAG (manual trigger) | Available on demand |
| 5 | Pool balance pipeline | Airflow DAG | Feeds the daily card balance charts |
| 6 | NASP disbursements materialized view | PostgreSQL materialized view | Powers the NASP dashboard |
| 7 | NASP Disbursements – Average Amount by Benefits dashboard | Metabase dashboard | In use |
| 8 | Card Transactions & Balance dashboard | Metabase dashboard, also in the web app | In use |
| 9 | P2P Commission dashboard | Metabase dashboard | In use |
| 10 | Concurrent refresh of the card debit-mix view | Airflow task, merge request | Raised for review |
| 11 | Dashboard QA: 16 issues logged and tracked | Oxigen | Logged and assigned |
| 12 | Weekly Business Report validation, documentation, handover | Reporting | Completed |

Sections 4 to 10 describe each piece of work in detail.

---

## 4. Data-quality monitoring

### 4.1 The problem

Dashboards do not show an error when an upstream feed breaks. They quietly show less activity,
and the gap is often noticed only weeks later, when someone questions a number. The team already
had a health check for the **database** (size, locks, connections), but nothing that watched the
**data** inside each table.

### 4.2 Data contracts kept in a table, not in code

I designed a **data-contract table** that holds one row of rules per monitored table:

- **Freshness:** how many days behind today the newest usable day may be.
- **Row volume:** the minimum and maximum number of rows in a normal day.
- **Values:** the allowed range of one key metric, with an optional filter so that documented
  placeholder values are not reported as errors.
- **Severity:** CRITICAL or WARN.
- **Suppression:** an optional date until which alerts are muted, which must always come with a
  written reason.
- **Reasoning:** a plain-language explanation of why every threshold has the value it has.

Because the rules live in a table, a threshold can be changed with a single update and a reason,
without redeploying any code. I wrote and justified contracts for four application-analytics
feeds: app usage events, app install figures, crash reports and store ratings. Every threshold
was measured from the live tables, not guessed.

### 4.3 The `data_contract_check` pipeline

An Airflow DAG runs every day at **08:00 Tashkent time**, at the start of the working day:

1. **load_contracts** reads the active contracts and narrows them to the tables in scope.
2. **pick_contracts** passes them on.
3. **check_contract** is a *mapped* task: Airflow creates one task instance per contract at run
   time, so adding a row to the table adds a check with no code change.
4. **report** turns the results into findings and decides whether anyone needs to be told.

Key design decisions:

- **Freshness is not "the newest date present".** A broken feed rarely stops cleanly; it
  trickles one or two rows a day, which looks healthy if you only check the latest date. A day
  counts as fresh only if it holds at least the minimum number of rows. The report shows both
  dates side by side, so the gap is visible.
- **Volume and values are checked on the newest day present.** Checking volume on the "credible"
  day would be pointless, because that day was chosen for clearing the minimum. Checking the
  newest day names a partial load as a partial load.
- **The monitor cannot damage what it monitors.** It connects read-only, so the database itself
  refuses any write. Its alert state is stored in Airflow, not in the database it watches, so it
  still works when that database is down.
- **Nothing disappears silently.** Muted contracts are still checked and listed in the daily
  summary, and the mute expires on its own. Tables left out of scope are named as "not checked".
  If the rules cannot be read at all, that is reported as a critical finding instead of a green
  run.
- **`all_done` trigger rule.** When there is nothing to check, the mapped task is skipped, and
  under Airflow's default rule the report step would be skipped too, so the "nothing was
  checked" alarm would never fire. Setting `all_done` makes it always run.

### 4.4 Baraka health-alerts Telegram bot

I created a **Telegram bot for Baraka health alerts** to deliver the findings, alongside Slack.
The alerting follows strict rules so the channel stays trustworthy:

- A problem is announced **once**, then muted until the set of problems changes or a 12-hour
  cooldown passes.
- A single **RECOVERED** message is sent when a problem clears.
- A quiet **daily summary** lists muted and unchecked tables.
- When everything passes and nothing is muted, the bot stays **silent**.
- If no message could actually be delivered, the cooldown is **not** started, so the next run
  tries again.

### 4.5 Testing

The task logic was run against the live warehouse with 17 test cases. They covered:
- the default scope and runtime overrides
- a typo in the scope
- an unreadable rules table and a contract pointing at a missing table
- cooldown, recovery and the silent path
- a failed delivery

---

## 5. P2P transfer pipelines

### 5.1 P2P channel gold pipeline (`p2p_channel_daily_gold`)

This pipeline produces the gold tables behind P2P reporting: daily P2P activity broken down by
channel (**Baraka**, **Xazna** and **Zoomrad**), in Uzbek so'm. It runs daily at **07:00
Tashkent time**, after the upstream loads it depends on.

**Two gold tables, maintained together:**

- a **daily aggregate**, one row per day and channel, with transaction count, amount, commission
  and distinct users;
- a **user-day bridge**, one row per active card per day and channel. It gives exact
  distinct-user counts for any week, month or year, which cannot be obtained by adding up daily
  counts.

**What I worked on:**

- **Moving the Baraka channel to the system of record.** Baraka's own transfers were re-sourced
  from the bank card ledger to the core banking P2P ledger. The card ledger often delivered only
  part of a day, and days that were still short when they left the rebuild window stayed wrong
  for good. The swap was validated month by month against the old method before it was made.
- **Time zones.** Core banking stores times in UTC, but the business day is Tashkent (UTC+5).
  Bucketing by UTC date files the evening hours, roughly an eighth of each day, under the wrong
  date. The pipeline shifts the window bounds instead of converting every row, so the index can
  still be used.
- **Avoiding double counting.** Baraka transfers also appear in the card ledger under the same
  label as another channel, so a terminal registry is used to exclude them. The pipeline refuses
  to run if that registry is empty.
- **Clean definitions.** Only completed transfers count. Reversals, failed transfers, incoming
  money and the duplicate second leg of same-bank transfers are excluded, and the reason for each
  exclusion is documented.
- **Commission.** A commission measure was added and reconciled to the ledger. It is zero by
  construction on the two channels where Baraka earns no fee, and this is checked on every run.
- **Rolling rebuild window.** Each run deletes and rebuilds the last 7 days inside one stored
  procedure call, so late or updated rows are picked up and a failure rolls back cleanly. The same
  procedure, called with a wider window, rebuilds the full history, so the daily and backfill
  results cannot drift apart.

**Checks on every run:**

- *Before rebuilding:* the source must be fresh and the terminal registry must be populated.
- *After rebuilding:* every channel label must be valid and all three channels must be present.
- The Baraka transaction count and commission must match the raw ledger **exactly**.
- Daily distinct users must equal the bridge's count for each day and channel.

The run then logs a per-channel summary of the previous day.

### 5.2 P2P daily summary pipeline (`p2p_daily_summary`)

A manually triggered companion pipeline. It produces one row per Tashkent day with total amount,
commission, total transactions and successful transactions, combining the legacy and current
P2P ledgers. A `lookback_days` parameter controls how many days are recomputed, so the same
pipeline serves a quick refresh and a full-history rebuild. Re-runs update existing rows instead
of duplicating them.

### 5.3 Card debit-mix refresh (merge request)

I added a task that refreshes the card debit-mix materialized view **concurrently** after the
rebuild window. A concurrent refresh keeps dashboards readable while the view rebuilds, and it
relies on a unique index on the view. The task uses the `all_done` trigger rule so the refresh
still runs if an earlier step reports a problem. It was raised as a GitLab merge request for
review.

---

## 6. NASP disbursement analytics

### 6.1 NASP disbursements materialized view

A materialized view with **one row per NASP benefit disbursement**, built to make the NASP
dashboard fast and consistent. For each payment it:

- **identifies benefit payouts** among card credits by their transaction type, excluding rows
  the bank later deleted;
- **links the payment to the recipient's personal identification number (PINFL)** through the
  card registry, handling the two banks' different card identifiers;
- **attributes the benefit(s)** active on the payment date, allowing a 35-day grace period after
  a benefit's end date, and labels each payment *single*, *multiple* or *none*, with a reason
  for *none* (no record, record not started or record expired);
- **adds the recipient's region**;
- **converts the amount to USD** at the exchange rate in effect on the payment date, falling back
  to the earliest rate for older payments.

A unique index allows the view to be refreshed concurrently, and further indexes support the
date and PINFL filters.

### 6.2 NASP Disbursements – Average Amount by Benefits dashboard

I built the Metabase dashboard on top of this view. It has eight cards:

| Card | What it answers |
|---|---|
| Total disbursed (USD) | How much was paid in the selected period |
| Number of disbursements | How many payments were made |
| Unique recipients | How many people were paid |
| People on two or more benefits | How many people receive more than one benefit |
| Average amount per disbursement by benefit | Which benefits pay the most per payment, with the median alongside the mean |
| Monthly trend by benefit | How payments per benefit change month to month |
| Benefit summary statistics | Recipients, total, mean, median, 90th percentile, minimum, maximum, average per recipient-month and payments per recipient-month, with a flag where the mean is more than 15% above the median |
| Top 25 recipients | The largest recipients, marked when they receive two or more benefits |

All cards share the same base rules:
- every payment gets a clear benefit label;
- amounts below USD 1 are excluded;
- payouts belonging to a separate social-insurance request list are excluded when the person had
  no active NASP benefit at the time.

Users can filter by date, bank, region, benefit and PINFL.

---

## 7. Card transaction and balance analytics

### 7.1 Card Transactions & Balance dashboard

I built the **Card Transactions & Balance** dashboard and wrote the SQL behind its cards. It is
available in Metabase and inside the Baraka web application under *Extended dashboard*, so NASP
and management users can use it without opening Metabase. It covers:

- **Drill-down summary:** total credited and debited amounts in USD, each split by bank, with
  bank and date filters.
- **Credit breakdown:** how money enters the cards (NASP disbursements, incoming P2P transfers
  and other credits).
- **Debit breakdown:** how money leaves (P2P transfers, cash withdrawals, POS purchases, mobile
  payments and other).
- **Trends:** daily credits, NASP disbursements and the matching debit trend. The debit trend
  showed that beneficiaries move or withdraw a large share of their money within a day or two
  of payment.
- **Payment disbursement by bank and MCC analysis:** spending by merchant category. It showed
  that most credits were uncategorised, which pointed to where the merchant-category mapping
  could be improved.
- **Daily total card balance** for each bank. The balance rises after each disbursement cycle
  and falls gradually as people spend.

### 7.2 Pool balance pipeline

An Airflow pipeline that computes the **daily total card balance for each issuing bank**,
converts it to USD and upserts it into a gold table. The daily balance charts read from that
table.

---

## 8. P2P commission dashboard

The **P2P-Commission** dashboard tracks the commission charged on card-to-card transfers. It
shows:

- four summary cards: total commission in UZS and in USD, total transactions and successful
  transactions;
- a daily commission trend;
- a monthly trend comparing transaction counts with commission;
- a "last updated" card.

It reads the legacy core banking P2P ledger, which stops when the current ledger took over in
May 2026.

*Note for me:* the ledger stores amounts in tiyin (1/100 so'm), so commission totals must
be divided by 100 before being shown as so'm or converted to USD.

---

## 9. Dashboard quality assurance

Before stakeholders relied on the dashboards, I tested them and logged every problem in
**Oxigen**, the company's issue-tracking platform. I recorded **16 issues** as subtasks of one
feature ticket for dashboard issues. Each was assigned to the developer responsible and tracked
there. The issues covered:

- **Card metrics:** card metrics for MyGov, and the agent-app report showing on the citizen-app
  card metrics.
- **Card transaction and balance charts:** five charts, and the dashboard not updating daily.
- **CBO overview:** a chart error and a stale "last available date".
- **Payments:** the Paynet transaction-sum card.
- **Filters:** the district filter on benefit applications and the age filter on social payouts.
- **Search:** admin search, NASP admin name and PINFL search.
- **Insights dashboard:** failed data export and the OTP SMS trend.
- **Development insights:** charts for users by district and batch onboarding.
- **Weekly Business Report:** the CBO success-rate metric not updating.

## 10. Reporting, documentation and handover

- **Weekly Business Report:** validated the weekly and monthly KPIs shown in the Baraka web
  application against the warehouse before publication.
- **Documentation:** documented the data contracts, the pipelines and the dashboards. Every
  threshold, exclusion and unit has a written reason next to it.
- **Code review:** submitted every change as a GitLab merge request and addressed the review
  comments before merging.
- **Handover:** ran a knowledge-transfer session with the team and a final review with the team
  lead.

---

## 11. Timeline

| Weeks | Dates (2026) | Focus |
|---|---|---|
| 1–2 | 23 Jun – 3 Jul | Induction, the Baraka programme and NASP, tool access, studying user, card, transaction and disbursement data |
| 3–5 | 6 – 24 Jul | Advanced SQL, query plans, materialized views, medallion layers, Airflow, GitLab workflow, Metabase basics, KPI definitions |
| 6 | 27 – 31 Jul | Currency and bank rules, MCC mapping, data-quality analysis, Oxigen walkthrough, contract design |
| 7–9 | 3 – 21 Aug | Data-contract framework: contracts, mapped checks, report and alerting, merge request, deployment, monitoring |
| 10–11 | 24 Aug – 4 Sep | P2P channel gold pipeline: channel and category definitions, gold aggregates, rebuild window, validation, deployment |
| 12 | 7 – 11 Sep | Pool balance pipeline |
| 13 | 14 – 18 Sep | Card Transactions & Balance dashboard |
| 14–15 | 21 Sep – 2 Oct | NASP materialized view and NASP Disbursements dashboard |
| 16–17 | 5 – 16 Oct | Dashboard performance, debit-mix concurrent refresh, Weekly Business Report validation, documentation |
| 18 | 19 – 20 Oct | Knowledge transfer and final review |

## 12. Engineering lessons

1. **Silence is the most dangerous failure.** A monitor that checks nothing while looking green
   is worse than no monitor. Every "nothing happened" path should say so explicitly.
2. **Measure thresholds from real data, and write the reason down.** A threshold without a
   reason gets loosened the first time it is inconvenient.
3. **Alerts must be rare and right.** Alert once, recover once, stay quiet otherwise. Otherwise
   people learn to ignore the channel.
4. **Know your units and time zones.** Minor currency units and UTC timestamps both cause
   silent errors that look like plausible numbers.
5. **Reconcile with the source of truth.** After every rebuild, prove the result matches the raw
   data instead of assuming it does.
6. **Make rebuilds idempotent and atomic.** Delete-and-reinsert a window inside one transaction,
   and use one code path for daily runs and backfills.
7. **Pre-compute for dashboards.** Materialized views and gold tables keep dashboards fast.
   Concurrent refresh keeps them available during rebuilds.
8. **Definitions matter more than queries.** Most of the work was agreeing what counts as a
   successful transfer, an active benefit or a fresh day. Once that was settled, the SQL was
   straightforward.

## 13. Skills gained

- Production SQL on large transaction tables: CTEs, window functions, percentiles, stored
  procedures, materialized views, indexes and query plans.
- Airflow pipeline design: dynamic task mapping, trigger rules, retries, pre-flight and
  post-run checks, idempotent rebuilds.
- Data-quality engineering: data contracts, freshness, volume and value checks, alert design.
- Business intelligence: Metabase dashboards with filters and drill-downs for non-technical
  users.
- Quality assurance: testing dashboards and writing clear, trackable issue reports.
- Professional practice: Git branching, merge requests, code review, documentation and handover.

---

