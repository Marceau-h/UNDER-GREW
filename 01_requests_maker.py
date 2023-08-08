import json
from pprint import pprint
import http.client
import urllib.parse
from time import sleep
from io import StringIO

import pandas as pd
from tqdm.auto import tqdm
from pathlib import Path

main = Path.cwd() / "exports"  # can be changed to a specific folder

# Caution, double quotes in the pattern must be escaped with a backslash
# And single quotes don't seem to work at all, better not using them
patterns = {
    'VERB': 'pattern {V [upos=VERB]}',
    'VERB-direct-obj': 'pattern {V [upos=VERB]; V -[obj]-> O}',
    'VERB-no-direct-obj': 'pattern {V [upos=VERB];} without {V -[obj]-> O}',
    'VERB-indirect-obj': 'pattern {V [upos=VERB]; V -[iobj]-> I}',
    'VERB-no-indirect-obj': 'pattern {V [upos=VERB];} without {V -[iobj]-> I}',
    'VERB-obl': 'pattern {V [upos=VERB]; V -[obl|obl:mod|obl:arg|obl:agent]-> I}',
    'VERB-no-obl': 'pattern {V [upos=VERB];} without {V -[obl|obl:mod|obl:arg|obl:agent]-> I}',
    'VERB-obj': 'pattern {V [upos=VERB]; V -[obj|iobj|obl]-> O}',
    'VERB-no-obj': 'pattern {V [upos=VERB];} without {V -[obj|iobj|obl|obl:mod|obl:arg|obl:agent|xcomp|ccomp]-> O}',
    'fixed-VERB': 'pattern {V [upos=VERB]; N []; N -[fixed]-> V}',
    'Verb_in_idiom': 'pattern { V [upos=VERB, InIdiom=Yes] }',
    'Verb-no-nothing': 'pattern {V [upos=VERB]} without {V -> O}',
    'Verb-then-punct': 'pattern {V [upos=VERB]; P [upos=PUNCT]; V > P}',
}

# This must be in the list of corpora available on the website
corpora = [
    'UD_French-FQB@2.12',
    'UD_French-GSD@2.12',
    'UD_French-PUD@2.12',
    'UD_French-ParTUT@2.12',
    'UD_French-Sequoia@2.12',
    'UD_French-ParisStories@2.12',
    'UD_French-Rhapsodie@2.12',
    'UD_Old_French-SRCMF@2.12',
    'WAC'
]

pivot = 'V'  # 'V' or 'O' or 'I' for tsv exports

# This is the connection to the website
conn = http.client.HTTPSConnection("gmb.marceau-h.fr")

pbar = tqdm(total=len(corpora) * len(patterns))
for corpus in corpora:
    pbar.set_description(f"Corpus {corpus}")
    corpus_folder = main / corpus.split('@')[0]
    corpus_folder.mkdir(exist_ok=True, parents=True)

    for pattern, pat_value in patterns.items():
        pat_file = corpus_folder / f"{pattern}.tsv"

        payload = """
        {"pattern":"%s",
        "corpus":"%s",
        "lemma":true,
        "upos":true,
        "xpos":false,
        "features":true,
        "tf_wf":false,
        "order":"init",
        "context":false,
        "clust1":"no",
        "clust2":"no"
        }""" % (pat_value, corpus)

        # payload = f"""
        # {{"pattern":{pat_value},",
        # "corpus":"{corpus}",
        # "lemma":true,
        # "upos":true,
        # "xpos":false,
        # "features":true,
        # "tf_wf":false,
        # "order":"init",
        # "context":false,
        # "clust1":"no",
        # "clust2":"no"
        # }}"""

        payload = f"param={urllib.parse.quote(payload)}"

        headers = {'content-type': "application/x-www-form-urlencoded"}

        conn.request("POST", "/search", payload, headers)

        res = conn.getresponse()
        data = res.read()

        data = json.loads(data.decode("utf-8"))

        # print(data)

        try:
            uuid = data["data"]["uuid"]
        except KeyError:
            print(pattern)
            print(data)
            raise

        # print(uuid)

        payload = f"param=%7B%22uuid%22%3A%22{uuid}%22%2C%22pivot%22%3A%22{pivot}%22%7D"

        conn.request("POST", "/export", payload, headers)

        res = conn.getresponse()
        data = res.read()

        assert json.loads(data.decode("utf-8"))["status"] == "OK", "Creation of export failed"
        sleep(1)  # wait for the export to be ready

        # print(data.decode("utf-8"))

        conn.request("GET", f"/data/{uuid}/export.tsv", "", headers)

        res = conn.getresponse()
        data = res.read()

        tsv = data.decode("utf-8")

        tsv = tsv.replace(r'"', r'""')

        with pat_file.open('w', encoding='utf-8') as f:
            f.write(tsv)

        try:
            df = pd.read_csv(pat_file, sep='\t', low_memory=False)

            df.to_csv(pat_file.with_suffix('.csv'), index=False)
            # df.to_excel(pat_file.with_suffix('.xlsx'), index=False)
            # df.to_pickle(pat_file.with_suffix('.pkl'))
            # df.to_json(pat_file.with_suffix('.jsonl'), orient='records', lines=True)
            # df.to_json(pat_file.with_suffix('.json'), orient='records')

        except pd.errors.ParserError:
            print(f"\nParsing error for {pat_file}\nfile of {len(tsv.splitlines())} lines")
            pbar.update(1)
            continue

        pbar.update(1)

