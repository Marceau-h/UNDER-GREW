import re
from io import StringIO
from pathlib import Path
from typing import Tuple, Dict, Any, List

import spacy
from tqdm.auto import tqdm

len_seg: int = 30_000

WAC: Path = Path(f"UD/WAC")
WAC.mkdir(exist_ok=True, parents=True)

spacy.require_gpu()

nlp: spacy.language = spacy.load("fr_dep_news_trf")

file: Path = Path("fra_mixed_2009_1M-sentences.txt")
with file.open("r", encoding="utf-8") as f:
    # The `int(i)` allows us to check if we are catching the sentence id correctly
    lines: Dict[int, str] = {int(i): l for i, l in [l.split("\t", 1) for l in f.readlines()]}

mixedticks = re.compile(r"'\"+'")
mixedspaces = re.compile(r'(\s)+')
manyticks = re.compile(r"\"{2,}")


def clean(s: str) -> str:
    s = s.strip()
    # s = re.sub(spaces, "\1", s)
    s = (
        s.replace(u"\x92", "'")
        .replace(u"\x9c", "œ")
        .replace(u"\xad", "")
        .replace("", "")
    )
    s = re.sub(mixedticks, "''", s)
    s = re.sub(mixedspaces, " ", s)
    s = re.sub(manyticks, '"', s)
    return s


def no_empty(s: str) -> str:
    return s if s else "_"


def get_all(sent: str) -> tuple[dict[str, int | list[Any] | str], ...]:
    doc: spacy.tokens.doc.Doc = nlp(sent)
    deps: List[str] = [token.dep_.lower() for token in doc]
    return tuple(
        {
            "ID": i + 1,
            "FORM": no_empty(token.text),
            "LEMMA": no_empty(token.lemma_),
            "UPOS": no_empty(token.pos_),
            "XPOS": no_empty(token.tag_),
            "FEATS": no_empty(token.morph),
            "HEAD": token.head.i + 1 if deps[i] != "root" else 0,
            "DEPREL": deps[i],
            "DEPS": f"{token.head.i + 1}:{no_empty(token.dep_)}" if deps[i] != "root" else "0:root",
            "MISC": "SpaceAfter=No" if not token.whitespace_ else "_",
        }
        for i, token in enumerate(doc)
    )


def process_segment(segment: Tuple[Tuple[int, str]]) -> None:
    first: int = segment[0][0]

    srtio: StringIO = StringIO()
    srtio.write("# global.columns = ID FORM LEMMA UPOS XPOS FEATS HEAD DEPREL DEPS MISC\n")

    for i, l in segment:
        lid: int = int(i)
        l: str = clean(l)

        if not l:
            print(f"Empty line at {lid}")
            print(l)
            raise ValueError

        srtio.write(f"# sent_id = {lid}\n")
        srtio.write(f"# text = {l}\n")

        for token in get_all(l):
            srtio.write("\t".join([str(v) for v in token.values()]) + "\n")
        srtio.write("\n")

        # if lid == 10:
        #     print(srtio.getvalue())

    with open(WAC / f"{first}_{lid}.conllu", "w", encoding="utf-8") as f:
        f.write(srtio.getvalue())

    srtio.close()


if __name__ == "__main__":

    segments: Tuple[Tuple[Tuple[int, str]]] = tuple(
        tuple(
            (
                k,
                lines[k],
            ) for k in range(i, i + len_seg) if k in lines
        ) for i in range(0, len(lines), len_seg)
    )

    for segment in tqdm(segments, total=len(segments)):
        process_segment(segment)
