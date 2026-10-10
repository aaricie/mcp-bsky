"""Self-contained checks for post image extraction (no login or network).

Run: python _test_post_images.py
"""
import ast
from pathlib import Path


class Obj:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


tree = ast.parse((Path(__file__).parent / "server.py").read_text(encoding="utf-8"))
wanted = {"_get_gallery_items", "_get_post_image_items"}
body = [
    node for node in tree.body
    if isinstance(node, ast.FunctionDef) and node.name in wanted
]
namespace = {}
exec(compile(ast.Module(body=body, type_ignores=[]), "server.py", "exec"), namespace)
extract = namespace["_get_post_image_items"]

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
