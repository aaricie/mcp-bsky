"""Choose which discussions the index names, and in what order.

Ranking, not covering. A covering objective (maximise units covered per token)
is degenerate here: a longer query strictly covers fewer units at strictly
higher cost, so greedy always converges on the cheapest word clearing whatever
gate is imposed — measured runs picked "oil", "video", "liaison", "juste".
Tightening the gate only changes which single word wins.

So each candidate query is treated as a *proposal* whose postlist is a candidate
discussion. Proposals are scored on how much discussion they represent and how
well they name it, then taken greedily against *marginal* mass, which is what
stops one big story from being emitted five times under five labels.

Two independent gates decide whether a postlist is a discussion at all, and a
proposal needs only one of them:

  coherence — its threads share more rare vocabulary than chance. Validated at
              AUC 0.993 for same-story on this capture. Catches news.
  lift      — it occurs far more here than the background firehose rate
              predicts. Catches conversation, where people share a subject
              ("odyssey") but not a vocabulary.

Requiring coherence alone silently drops every conversational topic, which on a
personal timeline is most of it; requiring lift alone lets in whatever the
timeline is merely fond of.
"""
from __future__ import annotations

import numpy as np
import scipy.sparse as sp


def saturate(x, lo, hi):
    return float(np.clip((x - lo) / (hi - lo), 0.0, 1.0))


def annotate(cands, uu, priors, bg, coh, lc, terms, *, head_terms=None):
    n = len(uu)
    npost = np.array([u.n_posts for u in uu], dtype=float)
    eng = np.array([u.likes + 2.0 * u.reposts for u in uu])
    # Damped per-unit mass: one merged mega-thread must not decide a ranking.
    umass = npost ** 0.6 + 0.45 * np.log1p(eng)
    subj = {t: priors.subjecthood(t, lc.get(t)) for t in terms}

    for c in cands:
        m = c["mask"]
        k = int(m.sum())
        c["units"] = k
        c["posts"] = int(npost[m].sum())
        c["eng"] = int(eng[m].sum())
        c["umass"] = umass
        c["mass"] = float(umass[m].sum())
        c["hits"] = bg.hits(c["q"])
        c["bits"] = bg.bits(c["q"])
        # A single-unit postlist has no pairs, so Coherence returns 1.0 —
        # which would hand every solo entry the maximum purity bonus. Score
        # them at a neutral value instead; they earn their place on thread size.
        c["coh"] = 0.05 if c.get("solo") is not None else coh.score(m, exclude=c["items"])
        c["nauth"] = len({h for i in np.flatnonzero(m) for h in uu[i].handles})

        p_bg = max(c["hits"], 0.5) / bg.N
        exp = n * (1.0 - (1.0 - p_bg) ** max(1.0, c["posts"] / max(k, 1)))
        c["lift"] = k / max(exp, 0.25)

        d = np.mean([saturate(subj[t], -0.6, 2.2) for t in c["q"]])
        if head_terms:
            d = min(1.0, d + 0.35 * np.mean([head_terms.get(t, 0.0) for t in c["q"]]))
        c["desc"] = float(d)
    return cands


def gate(cands, *, min_coh=0.040, min_lift=3.0, min_bits=8.3, min_units=2,
         min_authors=2, min_desc=0.35, solo_bits=10.5, min_bits_1term=11.5,
         n_units=None, small=400):
    """Keep only proposals that name a discussion.

    Single-term entries are held to a higher specificity bar than multi-term
    ones: a shorter label must be rarer to carry the same identifying
    information. That is what separates "hassett" and "bryansk" (fine alone)
    from "turkey" and "italy" (not).

    Corroboration requirements scale with the capture. Demanding two threads and
    two authors is right when there are 1,500 units and wrong when there are 80:
    a hand audit of a 100-post window found ~20 identifiable topics, of which
    the unscaled gate admitted two, because at that size most real discussions
    are a single thread by one or two people.
    """
    if n_units is not None and n_units < small:
        min_units, min_authors = 1, 1
        min_coh = min_coh * 0.6
        min_lift = min_lift * 0.6
    out = []
    for c in cands:
        one_term = len(c["q"]) == 1
        if c["bits"] < (min_bits_1term if one_term else min_bits):
            continue
        # A one-word entry has to stand alone as a topic name. Short fragments
        # ("cas", "ids", "bab") clear the rarity bar without naming anything.
        if one_term and len(c["q"][0]) < 4 and c["bits"] < 13.0:
            continue
        if c["desc"] < min_desc:
            continue
        if c.get("solo") is not None:
            # A lone thread earns a line on its own size, not on corroboration
            # from other threads, so coherence and author count do not apply.
            if c["bits"] < solo_bits or c["units"] > max(3, min_units + 2):
                continue
            out.append(c)
            continue
        if c["units"] < min_units or c["nauth"] < min_authors:
            continue
        if c["coh"] < min_coh and c["lift"] < min_lift:
            continue
        out.append(c)
    return out


def choose(cands, uu, *, budget=None, max_entries=None, overlap=0.55,
           redundancy=0.10, min_new_mass=0.6, w_spec=1.0, min_rel_score=0.08,
           per_entry_overhead=1, max_term_uses=2):
    """Greedy on *marginal* mass, so a story is named once rather than per facet.

    `min_rel_score` is a junk guard, not the main control: marginal value decays
    smoothly with rank (8.8 -> 2.1 by entry 80 -> 1.3 by 160 on the 6 h capture)
    and the tail is words like "cas", "ids", "indigo" rather than discussions.
    A floor relative to the leader is scale-free, which matters because a 72 h
    window holds several times as many discussions as a 6 h one; the token
    budget is what sets the operating point.

    `per_entry_overhead` charges each entry for its separator, so `budget` is
    the size of the rendered artifact rather than of the labels alone.

    `max_term_uses` caps how many entries may share a word. Marginal mass alone
    does not stop a big story fragmenting: a conjunction can only ever retrieve
    an intersection, so one story legitimately produces many partly-disjoint
    postlists and the measured index carried ten lines for one jobs report
    ("jobs july", "graph jobs", "fox jobs", "jobs low", "economy jobs", ...).
    Capping reuse of the shared word spends those tokens on other discussions.
    """
    if not cands:
        return [], 0
    n = len(uu)
    umass = cands[0]["umass"]
    used = np.zeros(n, dtype=np.float32)
    chosen, spent, taken = [], 0, []
    term_uses = {}

    quality = np.array([
        c["desc"]
        * max(c["coh"], 0.004) ** 0.20
        * min(c["lift"], 8.0) ** 0.25
        * (0.55 + 0.45 * saturate(c["bits"], 8.0, 12.0)) ** w_spec
        / c["cost"] ** 0.20
        for c in cands])
    alive = np.ones(len(cands), dtype=bool)

    # One sparse mat-vec per iteration instead of a Python loop over candidates.
    # At 72 h (12k units, 20k candidates) the loop version spent minutes here.
    C = sp.csr_matrix(np.array([c["mask"] for c in cands], dtype=np.float32))
    cost_arr = np.array([c["cost"] for c in cands], dtype=np.float32)

    while alive.any():
        avail = umass * (1.0 - used * (1.0 - redundancy))
        mm = C @ avail
        s = np.where(alive, mm ** 0.7 * quality, -1.0)
        s = np.where(mm >= min_new_mass, s, -1.0)
        if budget is not None:
            s = np.where(cost_arr + per_entry_overhead <= budget - spent, s, -1.0)
        best_i = int(np.argmax(s))
        best_s = float(s[best_i])
        if best_s <= 0:
            break
        if chosen and best_s < min_rel_score * chosen[0]["marginal"]:
            break
        alive[best_i] = False
        c = cands[best_i]
        m = c["mask"]
        if any(int((m & t).sum()) / max(1, min(int(m.sum()), int(t.sum()))) > overlap
               for t in taken):
            continue
        if max_term_uses and any(term_uses.get(t, 0) >= max_term_uses for t in c["q"]):
            continue
        c = dict(c)
        c["marginal"] = best_s
        chosen.append(c)
        for t in c["q"]:
            term_uses[t] = term_uses.get(t, 0) + 1
        taken.append(m)
        used[m] = 1.0
        spent += c["cost"] + per_entry_overhead
        if max_entries and len(chosen) >= max_entries:
            break
        if budget is not None and spent >= budget:
            break
    return chosen, spent
