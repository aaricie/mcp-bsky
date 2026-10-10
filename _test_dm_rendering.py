"""Offline checks for DM reply and shared-post rendering (real server.py, no network).

Run: python _test_dm_rendering.py
"""
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from _test_support import server


class Obj:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


namespace = vars(server)
render = namespace["_format_message_block"]


def post_embed(handle, text, embeds=None, uri="at://did:plc:author/app.bsky.feed.post/3shared"):
    record = Obj(
        py_type="app.bsky.embed.record#viewRecord",
        author=Obj(handle=handle),
        uri=uri,
        value=Obj(text=text),
        embeds=embeds,
    )
    return Obj(py_type="app.bsky.embed.record#view", record=record)


link = Obj(
    py_type="app.bsky.embed.external#view",
    external=Obj(title="An article", uri="https://example.com/a", description="Useful context"),
)
shared = post_embed("writer.example", "The shared post", [link])
message = Obj(
    py_type="chat.bsky.convo.defs#messageView",
    id="m1", sent_at=None, sender=Obj(did="did:alice"), text="Look at this",
    embed=shared, reply_to=None, reactions=None,
)
out = render(message, {"did:alice": "alice.example"}, "did:me", False)
assert "shared post | @writer.example" in out
assert "uri:at://did:plc:author/app.bsky.feed.post/3shared" in out
assert "The shared post" in out
assert "[Link] Title: An article | URL: https://example.com/a" in out

images = Obj(py_type="app.bsky.embed.images#view", images=[Obj(alt="a blue bird")])
parent = Obj(
    py_type="chat.bsky.convo.defs#messageView",
    id="parent1", sender=Obj(did="did:bob"), text="Original message",
    embed=post_embed("artist.example", "Post inside the reply", [images]),
)
reply = Obj(
    py_type="chat.bsky.convo.defs#messageView",
    id="m2", sent_at=None, sender=Obj(did="did:me"), text="My response",
    embed=None, reply_to=parent, reactions=[Obj(value="👍"), Obj(value="👍")],
)
out = render(reply, {"did:bob": "bob.example"}, "did:me", True)
assert "reply to @bob.example | id:parent1" in out
assert "Original message" in out
assert "shared post | @artist.example" in out
assert "uri:at://did:plc:author/app.bsky.feed.post/3shared" in out
assert "Post inside the reply" in out
assert "[1 image(s)] Alt: a blue bird" in out
assert "reactions: 👍x2" in out

for kind, marker in (
    ("viewNotFound", "[shared post deleted]"),
    ("viewBlocked", "[shared post blocked]"),
    ("viewDetached", "[shared post detached]"),
):
    unavailable = Obj(
        py_type="app.bsky.embed.record#view",
        record=Obj(py_type=f"app.bsky.embed.record#{kind}"),
    )
    msg = Obj(
        py_type="chat.bsky.convo.defs#messageView",
        id=kind, sent_at=None, sender=Obj(did="did:alice"), text="",
        embed=unavailable, reply_to=None, reactions=None,
    )
    assert marker in render(msg, {"did:alice": "alice.example"}, "did:me", False)

deleted_parent = Obj(py_type="chat.bsky.convo.defs#deletedMessageView")
msg = Obj(
    py_type="chat.bsky.convo.defs#messageView",
    id="m3", sent_at=None, sender=Obj(did="did:alice"), text="response",
    embed=None, reply_to=deleted_parent, reactions=None,
)
assert "reply to [deleted message]" in render(
    msg, {"did:alice": "alice.example"}, "did:me", False
)

print("DM rendering: 15 checks passed")
