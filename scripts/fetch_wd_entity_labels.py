import requests
import csv
import os
import time
import unicodedata


if __name__ == "__main__":

    url = "https://query.wikidata.org/sparql"
    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": "WikidataFetcher/1.0"
    }
    batch_size = 400
    last_qid = 0
    output_file = "wikidata_labels.csv"
    num_requests = 0
    max_requests = 10000
    checkpoint_file = "checkpoint.txt"

    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, "r") as f:
            last_qid = int(f.read().strip())

    write_header = not os.path.exists(output_file)

    with open(output_file, "a", newline="", encoding="utf-8-sig") as csvfile:
        writer = csv.writer(csvfile)
        if write_header:
            writer.writerow(["id", "label"])

        while num_requests < max_requests:
            values_str = " ".join(f"wd:Q{i}" for i in range(last_qid + 1, last_qid + 1 + batch_size))
            query = f"""
            SELECT ?entity (SAMPLE(?label) AS ?label) (SAMPLE(?alias) AS ?alias)

            WHERE {{
                VALUES ?entity {{ {values_str} }}

                OPTIONAL {{ ?entity rdfs:label ?label . FILTER(LANG(?label)="en") }}
                OPTIONAL {{ ?entity skos:altLabel ?alias . FILTER(LANG(?alias)="en") }}
            }}
            GROUP BY ?entity
            ORDER BY xsd:integer(STRAFTER(STR(?entity), "Q"))
            """

            response = requests.get(url, params={"query": query}, headers=headers)

            if response.status_code != 200:
                print(f"Error {response.status_code}: {response.text}")
                break
            
            response.encoding = "utf-8"
            data = response.json()["results"]["bindings"]

            if not data:
                print("No more results.")
                break

            for row in data:
                label_value = row.get("label", {}).get("value")
                alias_value = row.get("alias", {}).get("value")
                qid = row["entity"]["value"].split("/")[-1]
                label = ''
                if label_value and not 'Q' in label_value: # WD returns a Q-id for entities that dont exist
                    label = label_value
                elif label_value == '' and alias_value: 
                    label = alias_value

                if label != '':
                    label = unicodedata.normalize("NFC", label)
                    writer.writerow([qid[1:], label]) # save only numeric id
                    last_qid = int(qid[1:])

            print(f"Fetched up to Q{last_qid}")

            with open(checkpoint_file, "w") as f:
                f.write(str(last_qid))

            num_requests += 1
            time.sleep(8) # for rate limiting