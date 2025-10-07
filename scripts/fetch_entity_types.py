import csv
import pandas as pd
import requests
import time
import os


def id_to_int(wd_id):
    """
    Converts Wikidata ID like Q38830 or P31 to integer.
    """
    return int(wd_id[1:])

if "__main__":

    url = "https://query.wikidata.org/sparql"
    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": "WikidataFetcher/1.0"
    }

    print("[fetch_entity_types] Started")

    # Read entity IDs from file
    INPUT_FILE = 'wikidata_labels.csv' # has 2 columns: id,label for all enities in wikidata
    OUTPUT_FILE = 'entity_types_output.csv' 
    CHECKPOINT_FILE = 'entity_types_checkpoint.txt' # saves the last entity_id fetched
    BATCH_SIZE = 400
    LANG = 'en'

    df = pd.read_csv(INPUT_FILE, sep=None, engine="python", names=["id", "label"], header=None)
    df["id"] = df["id"].astype(str).str.strip()
    df["label"] = df["label"].fillna("").astype(str).str.strip()

    print(f"Found {len(df)} entities in {INPUT_FILE}")

    start_index = 0
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            last_qid = f.read().strip()
            if last_qid:
                # find the position of the last processed Q-ID
                if last_qid in df["id"].values:
                    start_index = df.index[df["id"] == last_qid][0] + 1
                    print(f"Resuming from after Q-ID {last_qid} (index {start_index})")
                else:
                    print(f"Checkpoint Q-ID {last_qid} not found in input file — starting from beginning.")
            else:
                print("Checkpoint file is empty — starting from beginning.")
    else:
        last_qid = 1
        print("No checkpoint file found — starting from beginning.")

    # start from checkpoint 
    df_to_process = df.iloc[start_index:].reset_index(drop=True)

    all_results = []

    for i in range(0, len(df_to_process), BATCH_SIZE):
        batch = df_to_process.iloc[i:i + BATCH_SIZE]
        values_str = " ".join(f"wd:Q{qid}" for qid in batch["id"])

        query = f"""
        SELECT ?entity ?class ?classLabel ?rank
        WHERE {{
            VALUES ?entity {{ {values_str} }}
            ?entity p:P31 ?statement.
            ?statement ps:P31 ?class.
            ?statement wikibase:rank ?rank.
          
            SERVICE wikibase:label {{
                bd:serviceParam wikibase:language "{LANG}".
            }}
        }}
        ORDER BY xsd:integer(STRAFTER(STR(?entity), "Q"))
        """ # need order by to ensure we can checkpoint correctly

        try:
            response = requests.get(url, params={'query': query, 'format': 'json'}, headers=headers)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch data for batch {i}-{i + BATCH_SIZE}: {e}")
            continue

        results = response.json().get("results", {}).get("bindings", [])

        for r in results:
            ent_id = str(r["entity"]["value"].split("/")[-1])
            c_id = str(r["class"]["value"].split("/")[-1])
            
            if 'Q' in ent_id and 'Q' in c_id:
                entity_id = id_to_int(ent_id) # save only id from Q-id
                class_id = id_to_int(c_id)# save only id from Q-id
                class_label = r["classLabel"]["value"]

                rank_wd = r["rank"]["value"].split("/")[-1].split(":")[-1].split('#')[-1]

                if rank_wd == "PreferredRank":
                    rank = "preferred"
                elif rank_wd == "DeprecatedRank":
                    rank = "deprecated"
                else:
                    rank = "normal"

                all_results.append({
                    "entity_id": entity_id,
                    "class_id": class_id,
                    "class_label": class_label,
                    "rank": rank
                })
                last_qid = entity_id

        # save to csv
        with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f_out:
            writer = csv.DictWriter(f_out, fieldnames=["entity_id", "class_id", "class_label", "rank"])
            if f_out.tell() == 0:
                writer.writeheader()
            writer.writerows(all_results)
            all_results = []

        with open(CHECKPOINT_FILE, "w") as f:
            f.write(str(last_qid))
            print('Fetched up to Q' + str(last_qid))

        time.sleep(5)

    print("Finished fetching entity types.")