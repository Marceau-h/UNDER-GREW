import json
from pprint import pprint
import http.client
import urllib.parse
import pandas as pd
from io import StringIO
from tqdm.auto import tqdm
from pathlib import Path

main = Path.cwd() # can be changed to a specific folder

# Caution, double quotes in the pattern must be escaped with a backslash
# And single quotes don't seem to work at all, better not using them
patterns = {
    'VERB': 'pattern {V [upos=VERB]}',
    'VERB-o': 'pattern {V [upos=VERB]; V -[obj]-> O}',
    'VERB-oi': 'pattern {V [upos=VERB]; V -[iobj]-> I}',
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
    'UD_Old_French-SRCMF@2.12'
]

pivot = 'V'  # 'V' or 'O' or 'I' for tsv exports

# This is the connection to the website
conn = http.client.HTTPSConnection("gmb.marceau-h.fr")

for corpus in tqdm(corpora):
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

        payload = f"param={urllib.parse.quote(payload)}"

        headers = {'content-type': "application/x-www-form-urlencoded"}

        conn.request("POST", "/search", payload, headers)

        res = conn.getresponse()
        data = res.read()

        uuid = json.loads(data.decode("utf-8"))["data"]["uuid"]

        # print(uuid)

        payload = f"param=%7B%22uuid%22%3A%22{uuid}%22%2C%22pivot%22%3A%22{pivot}%22%7D"

        conn.request("POST", "/export", payload, headers)

        res = conn.getresponse()
        data = res.read()

        assert json.loads(data.decode("utf-8"))["status"] == "ok", "Creation of export failed"

        # print(data.decode("utf-8"))

        conn.request("GET", f"/data/{uuid}/export.tsv", "", headers)

        res = conn.getresponse()
        data = res.read()

        tsv = data.decode("utf-8")

        with pat_file.open('w', encoding='utf-8') as f:
            f.write(tsv)

        try:
            df = pd.read_csv(pat_file, sep='\t', low_memory=False)

            df.to_csv(pat_file.with_suffix('.csv'), index=False)
            df.to_excel(pat_file.with_suffix('.xlsx'), index=False)
            df.to_pickle(pat_file.with_suffix('.pkl'))
            df.to_json(pat_file.with_suffix('.jsonl'), orient='records', lines=True)
            df.to_json(pat_file.with_suffix('.json'), orient='records')

        except pd.errors.ParserError:
            print(f"Erreur de parsing du fichier {pat_file}")
            continue

