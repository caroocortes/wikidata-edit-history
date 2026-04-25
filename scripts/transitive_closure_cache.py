import pandas as pd
import time
import sys
from pathlib import Path
import yaml
import pickle

from scripts.const import SETUP_PATH

class TransitiveClosureCache:
    def __init__(self):

        script_dir = Path(__file__).parent
        with open(script_dir.parent / Path(SETUP_PATH), 'r') as f:
            set_up = yaml.safe_load(f)

        csv_paths = {
            'subclass_transitive': set_up['transitive_closure_cache']['subclass_transitive_path'],
            'part_of_transitive': set_up['transitive_closure_cache']['part_of_transitive_path'],
            'has_part_transitive': set_up['transitive_closure_cache']['has_part_transitive_path'],
            'located_in_transitive': set_up['transitive_closure_cache']['located_in_transitive_path'],
        }

        transitive_closure_pickle_file_path = Path(set_up['transitive_closure_cache']['transitive_closure_pickle_file_path'])
        transitive_closure_stats_pickle_file_path = Path(set_up['transitive_closure_cache']['transitive_closure_stats_pickle_file_path'])
        self.cache = {}
        self.cache_stats = dict()

        stats_pickle_path = Path(transitive_closure_stats_pickle_file_path)

        if transitive_closure_pickle_file_path.exists() and stats_pickle_path.exists():
            print(f"Loading transitive closure cache from {transitive_closure_pickle_file_path}", flush=True)
            start_time = time.time()
            with transitive_closure_pickle_file_path.open('rb') as f:
                self.cache = pickle.load(f)
            print(f"Loaded transitive closure cache in {time.time() - start_time:.2f} seconds.", flush=True)
        
            print(f"Loading transitive closure stats from {transitive_closure_stats_pickle_file_path}", flush=True)
            start_time = time.time()
            if stats_pickle_path.exists():
                with stats_pickle_path.open('rb') as f:
                    self.cache_stats = pickle.load(f)
                print(f"Loaded transitive closure stats in {time.time() - start_time:.2f} seconds.", flush=True)
        else:
            print('Creating transitive closure cache from CSV files', flush=True)
            self.create_cache(csv_paths, transitive_closure_pickle_file_path, transitive_closure_stats_pickle_file_path)

    def create_cache(self, csv_paths, transitive_closure_pickle_file_path, transitive_closure_stats_pickle_file_path):

        start_time = time.time()
        for table_name, csv_path in csv_paths.items():
            if table_name not in self.cache_stats:
                self.cache_stats[table_name] = dict()
            start_time = time.time()
            self._load_csv(table_name, csv_path)
            self.cache_stats[table_name]['loading_time'] = time.time() - start_time
        print(f"Loaded all transitive closures in {time.time() - start_time:.2f} seconds.", flush=True)
        
        with transitive_closure_pickle_file_path.open('wb') as f:
            pickle.dump(self.cache, f)

        size_cache = sys.getsizeof(self.cache)
        print(f"Total transitive closure cache size: {size_cache / (1024 * 1024):.2f} MB", flush=True)
        for table_name, table_cache in self.cache.items():
            table_size = sys.getsizeof(table_cache)
            print(f" - {table_name} cache size: {table_size / (1024 * 1024):.2f} MB", flush=True)

            self.cache_stats[table_name]['num_rows'] = len(self.cache[table_name])
            self.cache_stats[table_name]['cache_size'] = table_size

            with transitive_closure_stats_pickle_file_path.open('wb') as f:
                pickle.dump(self.cache_stats, f)
    
    def _load_csv(self, table_name, csv_path):
        """Load transitive closure from CSV"""
        start_time = time.time()
        print(f"Loading {table_name} from {csv_path}", flush=True)

        df = pd.read_csv(csv_path)
        df.columns = df.columns.str.strip()
        df = df[['entity_id', 'transitive_closure_qids']]
            
        print('Building cache', flush=True)
        self.cache[table_name] = {}
        
        for _, row in df.iterrows():
            entity_id = row['entity_id']
            closure_qids_str = row['transitive_closure_qids']
            
            # Parse comma-separated QIDs into set
            if pd.notna(closure_qids_str) and closure_qids_str:
                qids_set = set(closure_qids_str.split(','))
            else:
                qids_set = set()
            
            self.cache[table_name][entity_id] = qids_set
        
        print(f"Loaded {len(self.cache[table_name])} entities for {table_name}. Took {time.time() - start_time:.2f} seconds.", flush=True)
    
    def check(self, value1, value2, table_name):
        """Check if value2 is in transitive closure of value1
        
            Example: to check if value1 is a subclass of value2, use table_name='subclass_transitive'
        """
        if table_name not in self.cache:
            return 0
        
        if value1 not in self.cache[table_name]:
            return 0
        
        cleaned_trans = [val.strip() for val in self.cache[table_name][value1]]

        return 1 if value2 in cleaned_trans else 0