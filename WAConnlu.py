import pandas as pd
import spacy
from tqdm.auto import tqdm
from io import StringIO
from pathlib import Path

WAC = Path("WAC")
WAC.mkdir(exist_ok=True, parents=True)

nlp = spacy.load("fr_core_news_sm")

file = "/home/marceau/Téléchargements/fra_mixed_2009_1M/fra_mixed_2009_1M-sentences.txt"

def get_all(sent):
    doc = nlp(sent)
    # headers = "ID FORM LEMMA UPOS XPOS FEATS HEAD DEPREL DEPS MISC".split()
    # for i, token in enumerate(doc, 1):
    #     yield token.i
    #     yield token.text
    #     yield token.lemma_
    #     yield token.pos_
    #     yield token.tag_
    #     yield "_"
    #     yield token.head.i + 1
    #     yield token.dep_
    #     yield "_"
    #     yield "_"
    #
    return [
        {
            "ID": i,
            "FORM": token.text,
            "LEMMA": token.lemma_,
            "UPOS": token.pos_,
            "XPOS": token.tag_,
            "FEATS": "_",
            "HEAD": token.head.i + 1,
            "DEPREL": token.dep_.lower(),
            "DEPS": "_",
            "MISC": "_"
        }
        for i, token in enumerate(doc, 1)
    ]

with open(file, "r", encoding="utf-8") as f:
    lines = f.readlines()

# lines = lines[:10000]
segments = []
for i in range(0, len(lines), 100000):
    end = i + 100000 if i + 100000 < len(lines) else len(lines)
    segments.append(lines[i:end])

for i, segment in enumerate(segments):
    pbar = tqdm(segment, total=len(segment))

    srtio = StringIO()
    srtio.write("# global.columns = ID FORM LEMMA UPOS XPOS FEATS HEAD DEPREL DEPS MISC\n")
    for i, l in enumerate(pbar):
        l = l.strip().replace(u"\x92", "'").replace(u"\x9c", "œ").replace(u"\xad", "")
        l = l.rsplit("\t", 1)
        srtio.write(f"# sent_id = {i}\n")
        srtio.write(f"# text = {l[1]}\n")
        for token in get_all(l[1]):
            srtio.write("\t".join([str(v) for v in token.values()]) + "\n")
        srtio.write("\n")

    with open(WAC / f"{i}.conllu", "w", encoding="utf-8") as f:
        f.write(srtio.getvalue())

