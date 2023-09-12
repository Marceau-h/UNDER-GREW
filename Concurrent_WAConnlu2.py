import re
from io import StringIO
from pathlib import Path
from multiprocessing import Pool, cpu_count
from time import sleep

import spacy
from tqdm.auto import tqdm


def clean(s: str) -> str:
    s = s.strip()
    s = s.replace(u"\x92", "'").replace(u"\x9c", "œ").replace(u"\xad", "").replace("", "")
    s = s.replace(r''''"''', "'").replace("''", "'")
    s = re.sub(r'[^"](.*)"', "\1", s)
    return re.sub(r'"+', '"', s)


def no_empty(s: str) -> str:
    return s if s else "_"


def get_all(sent: str, nlp: spacy.language) -> list[dict]:
    doc = nlp(sent)
    deps = [token.dep_.lower() for token in doc]
    return [
        {
            "ID": i + 1,
            "FORM": no_empty(token.text),
            "LEMMA": no_empty(token.lemma_),
            "UPOS": no_empty(token.pos_),
            "XPOS": no_empty(token.tag_),
            "FEATS": no_empty(token.morph),
            "HEAD": token.head.i + 1 if deps[i] != "root" else 0,
            "DEPREL": deps[i],
            "DEPS": "_",  # no_empty(token.dep_),
            "MISC": "SpaceAfter=No" if not token.whitespace_ else "_",
        }
        for i, token in enumerate(doc)
    ]


# def process_segment(segment: tuple, nlp: spacy.Language, WAC: Path):
def process_segment(args):
    segment, nlp, WAC = args
    first = None

    srtio = StringIO()
    srtio.write("# global.columns = ID FORM LEMMA UPOS XPOS FEATS HEAD DEPREL DEPS MISC\n")
    for i, l in segment:
        lid = int(i)

        if first is None:
            first = lid

        l = clean(l)

        if not l:
            print(f"Empty line at {lid}")
            print(l)
            raise ValueError

        srtio.write(f"# sent_id = {lid}\n")
        srtio.write(f"# text = {l}\n")

        for token in get_all(l, nlp):
            srtio.write("\t".join([str(v) for v in token.values()]) + "\n")
        srtio.write("\n")

    with open(WAC / f"{first}_{lid}.conllu", "w", encoding="utf-8") as f:
        f.write(srtio.getvalue())

        srtio.close()

    del segment
    del srtio
    del first


if __name__ == "__main__":
    len_seg = 30_000

    args = []

    file: Path = Path(r"C:\Users\marce\Downloads\fra_mixed_2009_1M\fra_mixed_2009_1M-sentences.txt")

    with file.open("r", encoding="utf-8") as f:
        # The `int(i)` allows us to check if we are catching the sentence id correctly
        lines: dict = {int(i): l for i, l in [l.rsplit("\t", 1) for l in f.readlines()]}

    segments = tuple(
        tuple(
            (
                k,
                lines[k],
            ) for k in range(i, i + len_seg) if k in lines
        ) for i in range(0, len(lines), len_seg)
    )

    for test in ('fr_core_news_sm', 'fr_core_news_md', 'fr_core_news_lg', 'fr_dep_news_trf'):
        WAC = Path(f"UD/WAC_{test}")
        WAC.mkdir(exist_ok=True, parents=True)

        nlp = spacy.load(test)

        for segment in segments:
            args.append((segment, nlp, WAC))

    with Pool(cpu_count() - 1) as p:
        for _ in tqdm(p.imap_unordered(process_segment, args), total=len(args)):
            pass
