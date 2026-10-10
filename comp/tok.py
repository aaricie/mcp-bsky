"""Token counting. o200k_base (GPT-4o/o200k) is used as the reproducible proxy for
Claude's tokenizer; both are modern byte-level BPEs with ~200k vocab and track each
other within a few percent on English social text."""
import functools

@functools.lru_cache(maxsize=1)
def _enc():
    import tiktoken
    return tiktoken.get_encoding("o200k_base")

def ntok(s: str) -> int:
    return len(_enc().encode(s, disallowed_special=()))
