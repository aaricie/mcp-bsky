"""Offline checks for post image extraction (real server.py, no login or network).

Run: python _test_post_images.py
"""
from _test_support import server


class Obj:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


extract = server._get_post_image_items

legacy = [Obj(alt="one"), Obj(alt="two")]
assert extract(Obj(embed=Obj(images=legacy))) == legacy

gallery = [Obj(alt=str(i)) for i in range(10)]
assert extract(Obj(embed=Obj(items=gallery))) == gallery

quoted_media = [Obj(alt="attached to quoting post")]
assert extract(Obj(embed=Obj(record=Obj(), media=Obj(items=quoted_media)))) == quoted_media

# A plain quoted record has no media attached to the requesting post.
assert extract(Obj(embed=Obj(record=Obj()))) == []
assert extract(Obj(embed=None)) == []

print("post image extraction: 5 checks passed")
