# CLM/CRF Discord Explainer Post
# Target channel: #cornerstone
# Tone: plain English, retail-investor-friendly, Gumroad "not signals, research" style
# Copy the block below and paste directly into Discord.

---

**Why most CLM/CRF holders leave money on the table — and how this system fixes it.**

Most people who hold CLM or CRF treat them like any other dividend ETF: buy, hold, collect the monthly check. That's fine. But it misses three mechanics that quietly compound against you — or, if you know them, quietly compound *for* you.

Here's what this system watches for, and why it matters.

---

**1 — The Rights Offering problem**

Every year or two, CLM and CRF raise capital by issuing new shares to existing holders at a discount. This is called a Rights Offering.

Sounds like a deal. It isn't.

The "discount" is priced below market, but still above NAV — the actual value of the fund's holdings. You pay a premium for shares worth less than you paid. Holders who don't participate get diluted. Holders who do participate overpay.

Neither is great.

The only real winner is someone who bought *before* the announcement, at the previous price. By the time the Rights Offering is public, the premium has already compressed. The market has repriced.

---

**2 — The N-2 filing: the only signal that matters**

Cornerstone must file with the SEC before they can run a Rights Offering. That filing is called an N-2.

It hits EDGAR — publicly available, zero cost to read — days to weeks before any press release. Before most retail holders know anything is coming.

This system watches EDGAR automatically. Every loop. When an N-2 appears for CLM (CIK 0000814083) or CRF (CIK 0000033934), an alert fires immediately.

That's the starting gun.

---

**3 — The dodge**

When the N-2 alert fires, the move is to sell 99% of the CLM/CRF position down to a minimum of 3 shares.

Three shares is the magic number. As long as you hold at least 3 shares, the DRIP program stays active permanently — you never have to re-enroll.

Then you wait.

The Rights Offering runs its course — typically 84 days from N-2 filing to expiration. During that window, CLM and CRF prices usually fall as institutional holders exit and arbitrageurs work the spread. Premium compresses from wherever it was down to near NAV.

When the dust settles, you rebuy. At lower prices than where you sold. With the same capital.

You end up with *more shares* than Rights Offering participants who subscribed at the discounted price. The dodge beats participation.

---

**4 — DRIP at NAV: built-in alpha every month**

This is the part most holders skip because it sounds like an accounting detail.

CLM and CRF reinvest dividends at NAV — the actual underlying value of the fund's holdings — not at the market price.

Because CLM and CRF almost always trade at a premium to NAV (meaning market price > NAV), your reinvested dividend buys more shares than it would if the DRIP used the market price.

Every month, you're getting a small structural edge. It adds up to roughly 23% additional share growth per year on top of the stated yield, compounded.

This is why the 3-share minimum rule exists. You never want to lose DRIP enrollment.

---

**5 — October NAV lock: the annual event that sets next year's income**

Cornerstone's Board of Directors meets every October and sets the distribution rate for the following year. The formula: 21% of end-of-October NAV.

If NAV is high in October → higher distributions next year.
If NAV is low in October → lower distributions next year.

This is why October is the highest-sensitivity month for CLM/CRF holders. A drop in NAV during October directly reduces next year's income. The 2026 distribution reset — which dropped from $0.1224/month to $0.1215/month for CLM — happened because October 2025 NAV closed lower than the year before.

Every Sunday in October, a reminder fires to reassess: Where is NAV today? What does that imply for next year's distribution? What's the FV target at 19% yield?

---

**What runs 24/7 in this channel:**

- SEC EDGAR watcher — N-2 and large-holder SC 13D/G filings, real-time
- Premium z-score — how elevated or compressed the premium is vs its historical range
- Dark pool detection — unexplained price drops on below-average public volume
- VIXY crisis overlay — VIX spike = CEF premium risk, flagged before it shows in price
- Institutional exit detector — high volume + flat SPY = someone large is leaving
- October NAV gate — heightened sensitivity all month, every October

This channel is where that data surfaces. Not signals. Research.

---

↳ *All CLM/CRF content stays in #cornerstone. Personal position data is private.*
