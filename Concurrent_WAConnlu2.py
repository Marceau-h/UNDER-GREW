import re
from io import StringIO
from pathlib import Path
from multiprocessing import Pool, cpu_count

import spacy
from tqdm.auto import tqdm

WAC = Path("UD/WAC_test")
WAC.mkdir(exist_ok=True, parents=True)

nlp = spacy.load("fr_core_news_sm")
# TODO: Try with the transformer model instead of the sm one (and maybe with the lg one too)
# spacy.prefer_gpu()
# nlp = spacy.load("fr_dep_news_trf")

file: Path = Path("/home/marceau/Téléchargements/fra_mixed_2009_1M/fra_mixed_2009_1M-sentences.txt")
# file: Path = Path(r"C:\Users\marce\Downloads\fra_mixed_2009_1M\fra_mixed_2009_1M-sentences.txt")


def clean(s: str) -> str:
    s = s.strip()
    s = s.replace(u"\x92", "'").replace(u"\x9c", "œ").replace(u"\xad", "").replace("", "")
    s = s.replace(r''''"''', "'").replace("''", "'")
    s = re.sub(r'[^"](.*)"', "\1", s)
    return re.sub(r'"+', '"', s)


def no_empty(s: str) -> str:
    return s if s else "_"


def get_all(sent: str) -> list[dict]:
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


def process_segment(segment: dict) -> None:
    first = None

    srtio = StringIO()
    srtio.write("# global.columns = ID FORM LEMMA UPOS XPOS FEATS HEAD DEPREL DEPS MISC\n")
    for i, l in segment.items():
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

        for token in get_all(l):
            srtio.write("\t".join([str(v) for v in token.values()]) + "\n")
        srtio.write("\n")

    with open(WAC / f"{first}_{lid}.conllu", "w", encoding="utf-8") as f:
        f.write(srtio.getvalue())


with file.open("r", encoding="utf-8") as f:
    # The `int(i)` allows us to check if we are catching the sentence id correctly
    lines: dict = {int(i): l for i, l in [l.rsplit("\t", 1) for l in f.readlines()]}

# lines = lines[:10000] ## For testing
len_seg = 30_000
segments: list[dict] = []
for i in range(0, len(lines), len_seg):
    sub = {k: lines[k] for k in range(i, i + len_seg) if k in lines}
    segments.append(sub)


with Pool(cpu_count()//2) as p:
    list(tqdm(p.imap(process_segment, segments), total=len(segments)))

