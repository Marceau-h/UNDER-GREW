import re

import pandas as pd
import spacy
from tqdm.auto import tqdm
from io import StringIO
from pathlib import Path

WAC = Path("UD/WAC")
WAC.mkdir(exist_ok=True, parents=True)

nlp = spacy.load("fr_core_news_sm")

# file = "/home/marceau/Téléchargements/fra_mixed_2009_1M/fra_mixed_2009_1M-sentences.txt"
file = r"C:\Users\marce\Downloads\fra_mixed_2009_1M\fra_mixed_2009_1M-sentences.txt"

def clean(s:str) -> str:
    s = s.strip()
    s = s.replace(u"\x92", "'").replace(u"\x9c", "œ").replace(u"\xad", "").replace("", "")
    s = s.replace(r''''"''', "'").replace("''", "'")
    s = re.sub(r'[^"](.*)"', "\1", s)
    return re.sub(r'"+', '"', s)

def no_empty(s:str) -> str:
    return s if s else "_"

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
    deps = [token.dep_.lower() for token in doc]
    return [
        {
            "ID": i+1,
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

with open(file, "r", encoding="utf-8") as f:
    lines = f.readlines()

# lines = lines[:10000]
segments = []
len_seg = 30_000
for i in range(0, len(lines), len_seg):
    end = min(len(lines), i + len_seg)
    segments.append(lines[i:end])

batch_first_sent_id = 1
for i, segment in enumerate(segments):
    batch_last_sent_id = batch_first_sent_id + len(segment) - 1

    pbar = tqdm(segment, total=len(segment), desc=f"Fichier {i+1}/{len(segments)} : ", leave=False)

    srtio = StringIO()
    srtio.write("# global.columns = ID FORM LEMMA UPOS XPOS FEATS HEAD DEPREL DEPS MISC\n")
    for j, l in enumerate(pbar):
        starting_line = l
        _, l = l.rsplit("\t", 1)

        l = clean(l)

        if not l:
            print(f"Empty line at {batch_first_sent_id + j}")
            print(starting_line)
            raise ValueError


        srtio.write(f"# sent_id = {batch_first_sent_id + j}\n")
        # srtio.write(f"# text = {l[1]}\n")
        srtio.write(f"# text = {l}\n")
        # for token in get_all(l[1]):
        for token in get_all(l):
            srtio.write("\t".join([str(v) for v in token.values()]) + "\n")
        srtio.write("\n")

    with open(WAC / f"{batch_first_sent_id}_{batch_last_sent_id}.conllu", "w", encoding="utf-8") as f:
        f.write(srtio.getvalue())

    batch_first_sent_id = batch_last_sent_id + 1
