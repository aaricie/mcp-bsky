"""Enumerate candidate keyqueries and their exact postlists.

Because Bluesky search is conjunctive, the postlist of a term conjunction is the
intersection of its terms' postlists — so mining frequent itemsets over
posts-as-transactions *is* simulating the search engine, not approximating it.

Candidates are generated per post rather than by descending a global lattice.
That is far faster (thousands of small combination sets instead of a search over
a 10k-term lattice), semantically right — a query should be built from the terms
of the discussion it names — and it is what keeps every candidate retrievable:
search matches one post at a time, so a conjunction proposed from a *thread's*
pooled vocabulary can name a thread that no single post satisfies, and such an
entry returns nothing at all.

Among all queries retrieving exactly the same set of discussions, only the
cheapest is kept. That is the minimality half of the keyquery definition, priced
in tokens rather than in words.
"""
from __future__ import annotations

import itertools
from collections import defaultdict

import numpy as np

from .vocab import term_cost


def mine(units_, terms, M, tidx, priors, local_cap, pidx, post_terms, *,
         per_post=14, max_len=3, min_sup=3, max_cost=7):
    """Return candidate dicts: q (term ids), sup, mask, cost.

    `min_sup` is applied twice: first to how many posts propose the itemset
    (cheap), then to how many discussion units its true postlist reaches.
    """
    n = len(units_)
    costs = np.array([term_cost(t) for t in terms], dtype=np.int32)
    subj = np.array([priors.subjecthood(t, local_cap.get(t)) for t in terms])
    df = M.sum(1)

    # Per post, keep the terms that best identify it: rare in the timeline and
    # subject-like, per token spent.
    idf = np.log(n / np.maximum(df, 1))
    value = (idf + 0.6 * np.clip(subj, -2, 4)) / costs

    support: dict[tuple, int] = defaultdict(int)
    for ts in post_terms:
        ids = [tidx[t] for t in ts if t in tidx]
        if not ids:
            continue
        ids.sort(key=lambda i: -value[i])
        ids = sorted(ids[:per_post])
        for L in range(1, max_len + 1):
            if L > len(ids):
                break
            for c in itertools.combinations(ids, L):
                if costs[list(c)].sum() <= max_cost:
                    support[c] += 1

    cands = []
    seen_closure: dict[bytes, dict] = {}
    for c, s in support.items():
        if s < min_sup:
            continue
        mask = pidx.mask_for(c)
        sup = int(mask.sum())
        if sup < min_sup:
            continue
        cost = int(costs[list(c)].sum())
        key = mask.tobytes()
        prev = seen_closure.get(key)
        if prev is None or cost < prev["cost"] or (
                cost == prev["cost"] and subj[list(c)].sum() > prev["subj"]):
            seen_closure[key] = dict(items=c, mask=mask, sup=sup, cost=cost,
                                     subj=float(subj[list(c)].sum()),
                                     q=[terms[i] for i in c])
    cands = list(seen_closure.values())
    return cands


def solo(units_, terms, M, tidx, priors, local_cap, pidx, post_terms, *,
         min_posts=4, eng_rank_frac=0.08, eng_rank_min=20, max_len=3,
         max_cost=7, per_unit=10, top_per_unit=6):
    """Candidates for discussions that are one big thread rather than many posts.

    A long conversation is a single unit, so no conjunction reaches support 2 and
    the multi-unit path never sees it — yet a 46-post argument about what
    capitalism is, or a 15-post thread on generative-AI ads, is exactly what
    "what's being discussed" means on a personal timeline. These units get their
    own candidates, generated from their own distinctive vocabulary.

    Admission is by *rank within this capture*, never an absolute number. A fixed
    engagement threshold silently stops working on short windows — over 100 posts
    a 451-like video of an arrest and a 395-like post were the two things the
    timeline reacted to most, and both fell under a hard `min_eng=500`. And what
    counts as a discussion is scale-relative anyway: a lone post is noise among
    3,000, but among 100 it is a real share of what the reader saw.
    """
    costs = np.array([term_cost(t) for t in terms], dtype=np.int32)
    subj = np.array([priors.subjecthood(t, local_cap.get(t)) for t in terms])
    df = M.sum(1)
    n = M.shape[1]
    idf = np.log(n / np.maximum(df, 1))
    value = (idf + 0.6 * np.clip(subj, -2, 4)) / costs

    engs = np.array([u.likes + 2 * u.reposts for u in units_], dtype=float)
    k = min(len(engs), max(eng_rank_min, int(eng_rank_frac * len(engs))))
    top_eng = set(np.argsort(-engs)[:k].tolist()) if k else set()

    out = []
    for j, u in enumerate(units_):
        if u.n_posts < min_posts and j not in top_eng:
            continue
        # Within-unit term frequency is what separates the subject from the
        # scenery. Every term of a lone thread is equally rare corpus-wide, so
        # ranking by IDF alone picks whichever proper noun happens to be
        # mentioned once — measured labels were "bytes greg" for an argument
        # about capitalism and "ajax matt" for a thread about the Odyssey film.
        # A subject recurs across the thread; a passing name does not.
        # A one- or two-post unit has tf == 1 for every term, so a hard `tf >= 2`
        # rejects it outright — which silently dropped every high-engagement
        # single post, the 451-like arrest video among them.
        need = 1 if u.n_posts < 3 else max(2, int(0.18 * u.n_posts))
        ids = [tidx[t] for t in u.terms
               if t in tidx and u.tf.get(t, 0) >= need]
        if len(ids) < 2:
            ids = [tidx[t] for t in u.terms if t in tidx]
        if len(ids) < 2:
            continue

        # Corpus IDF is the wrong ranking for a lone thread — every one of its
        # terms is equally rare corpus-wide, so IDF just surfaces the rarest
        # incidental noun. What identifies the subject is how much of the
        # thread uses the term, whether it is subject-like, and whether the
        # root post (which sets the topic) contains it.
        root = u.root_terms if hasattr(u, "root_terms") else set()

        def solo_value(i):
            t = terms[i]
            tfn = u.tf.get(t, 0) / max(u.n_posts, 1)
            s = float(np.clip((subj[i] + 0.6) / 2.8, 0.15, 1.0))
            return ((0.22 + tfn) * s * min(idf[i], 6.0)
                    * (1.25 if t in root else 1.0) / costs[i])

        ids.sort(key=lambda i: -solo_value(i))
        ids = ids[:per_unit]
        # Only conjunctions that some single post of this thread satisfies —
        # anything else is unretrievable however well it describes the thread.
        within = [set(post_terms[p]) for p in u.post_rows]
        scored = []
        for L in range(2, max_len + 1):
            for c in itertools.combinations(ids[:8], L):
                cost = int(costs[list(c)].sum())
                if cost > max_cost:
                    continue
                qs = {terms[i] for i in c}
                if not any(qs <= w for w in within):
                    continue
                mask = pidx.mask_for(c)
                if not mask[j]:
                    continue
                scored.append((float(sum(solo_value(i) for i in c)),
                               dict(items=tuple(sorted(c)), mask=mask,
                                    sup=int(mask.sum()), cost=cost,
                                    subj=float(subj[list(c)].sum()),
                                    q=[terms[i] for i in sorted(c)], solo=j)))
        scored.sort(key=lambda x: -x[0])
        out += [d for _, d in scored[:top_per_unit]]
    return out
