import re
from io import StringIO
from pathlib import Path
from typing import Tuple, Dict, Any, List

import spacy
from tqdm.auto import tqdm

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
    """
    Cleans the sentence string from unwanted characters including multiple spaces and multiple ticks but also converts
    bad codepoints to their correct unicode character
    :param s: the sentence string to clean
    :return: the cleansed sentence string, ready to be processed by spacy
    """
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
    """
    Returns "_" if the string is empty, else returns the string. Used to fill empty fields in CoNLL-U format
    :param s: the string to check
    :return: "_" if the string is empty, else returns the string
    """
    return s if s else "_"


def get_all(sent: str) -> tuple[dict[str, int | list[Any] | str], ...]:
    """
    The moon of the show, processes a sentence with spacy and returns a tuple of dicts, each dict representing a token
    with every field of the CoNLL-U format
    :param sent: the sentence to process
    :return: a tuple of dicts, each dict representing a token with every field of the CoNLL-U format
    """

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
    """
    The star of the show, processes a segment of sentences with spacy and writes the result to a newly created
    CoNLL-U file
    :param segment: the segment to process, a tuple of tuples of sentence id and sentence
    :return: None
    """
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


def main(start: int = 0, end: int = len(lines), len_seg: int = 30_000) -> None:
    """
    Main function, creates the segments and processes them through `process_segment`
    :param start: the first sentence id to process
    :param end: the last sentence id to process
    :param len_seg: the length of each segment
    :return: None
    """

    segments: Tuple[Tuple[Tuple[int, str]]] = tuple(
        tuple(
            (
                k,
                lines[k],
            ) for k in range(i, i + len_seg) if k in lines
        ) for i in range(start, end, len_seg)
    )

    for segment in tqdm(segments, total=len(segments)):
        process_segment(segment)


if __name__ == "__main__":
    main()
