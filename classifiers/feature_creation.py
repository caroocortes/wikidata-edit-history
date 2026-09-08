import re
from Levenshtein import distance as levenshtein_distance
import os
import numpy as np
from sentence_transformers import SentenceTransformer, CrossEncoder
import torch
import re
import Levenshtein
import difflib
from parser_scripts.utils import strip_accents, is_plural_pair, has_adjacent_swap, normalize_for_residual
from parser_scripts.const import *
import gc

class FeatureCreation():

    def __init__(self, set_up=None, conn=None):
        self.conn = conn
        self.set_up = set_up

        self.rule_base_time = 0
        self.feature_creation_time = 0

    def create_embedding_features(self, df, old_col, new_col):
        """
            Calculates cosine similarity between old and new value embeddings.
            If descriptions are missing we forced 0.0 similarity.
            For labels we don't need this since we always have labels for entity changes.
        """

        def _row_cosine_similarity(a_embeddings, b_embeddings):
            # vectorized row-wise cosine similarity
            a = np.asarray(a_embeddings)
            b = np.asarray(b_embeddings)
            a_norm = a / np.clip(np.linalg.norm(a, axis=1, keepdims=True), 1e-12, None)
            b_norm = b / np.clip(np.linalg.norm(b, axis=1, keepdims=True), 1e-12, None)
            return np.sum(a_norm * b_norm, axis=1)

        old_texts = []
        new_texts = []

        old_description = []
        new_description = []
        desc_valid = []

        old_label = []
        new_label = []

        device = "cuda" if torch.cuda.is_available() else "cpu"

        print(f"Using device: {device} for embedding and NLI models", flush=True)

        nli_model = getattr(self, 'nli_model', None)
        if nli_model is None:
            self.nli_model = CrossEncoder('cross-encoder/nli-deberta-v3-base', device=device)

            if device == "cuda":
                self.nli_model.model.half()

        embedding_model = getattr(self, 'embedding_model', None)
        if embedding_model is None:
            self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2')

        def _clean_series(s):
            return s.astype(str).str.replace('"', '', regex=False)

        def _is_empty_series(s):
            s_str = s.astype(str).str.strip()
            return s.isna() | (s_str == '') | (s_str.str.lower() == 'nan') | (s is None)

        if 'label' in old_col:
            old_label = _clean_series(df[old_col]).tolist() # this are the QIDs and they are in JSONB so they have "" before and after
            new_label = _clean_series(df[new_col]).tolist()

            old_empty = _is_empty_series(df['old_value_description'])
            new_empty = _is_empty_series(df['new_value_description'])
            desc_valid = (~old_empty & ~new_empty).tolist()

            old_description = df['old_value_description'].astype(str).where(~old_empty, '').tolist()
            new_description = df['new_value_description'].astype(str).where(~new_empty, '').tolist()
        else:
            old_texts = _clean_series(df[old_col]).tolist()
            new_texts = _clean_series(df[new_col]).tolist()

        if 'label' not in old_col:  # remove for entity
            print("Creating embedding features for text changes", flush=True)
            old_text_embeddings = self.embedding_model.encode(
                old_texts,
                device=device,
                show_progress_bar=True,
                batch_size=512
            )
            new_text_embeddings = self.embedding_model.encode(
                new_texts,
                device=device,
                batch_size=512,
                show_progress_bar=True
            )
            similarities = _row_cosine_similarity(old_text_embeddings, new_text_embeddings)
            similarities = np.clip(similarities, -1.0, 1.0)
            df['value_cosine_similarity'] = similarities

            del old_text_embeddings, new_text_embeddings
            gc.collect()
            torch.cuda.empty_cache()

            # --- Directional NLI entailment scores (old<->new specificity direction) ---

            o2n_pairs = list(zip(old_texts, new_texts))
            n2o_pairs = list(zip(new_texts, old_texts))

            cols_o2n = ['old_to_new_contradiction', 'old_to_new_entailment', 'old_to_new_neutral']
            cols_n2o = ['new_to_old_contradiction', 'new_to_old_entailment', 'new_to_old_neutral']

            if o2n_pairs:
                print("Creating NLI features for text changes", flush=True)
                o2n_scores = self.nli_model.predict(o2n_pairs, 
                                                    batch_size=128, 
                                                    show_progress_bar=True
                                                )
                n2o_scores = self.nli_model.predict(n2o_pairs, 
                                                    batch_size=128,
                                                    show_progress_bar=True
                                                )

                df.loc[df.index, cols_o2n] = np.asarray(o2n_scores)
                df.loc[df.index, cols_n2o] = np.asarray(n2o_scores)
            else:
                df.loc[df.index, cols_o2n] = np.nan
                df.loc[df.index, cols_n2o] = np.nan

            del o2n_pairs, n2o_pairs
            gc.collect()
            torch.cuda.empty_cache()

        if 'label' in old_col:
            print("Creating embedding features for entity changes", flush=True)
            old_label_embeddings = self.embedding_model.encode(
                old_label,
                device=device,
                show_progress_bar=True,
                batch_size=512
            )
            new_label_embeddings = self.embedding_model.encode(
                new_label,
                device=device,
                show_progress_bar=True,
                batch_size=512
            )
            similarities = _row_cosine_similarity(old_label_embeddings, new_label_embeddings)
            similarities = np.clip(similarities, -1.0, 1.0)
            df['label_cosine_similarity'] = similarities

            del old_label_embeddings, new_label_embeddings
            gc.collect()
            torch.cuda.empty_cache()

            old_description_embeddings = self.embedding_model.encode(
                old_description,
                device=device,
                show_progress_bar=True,
                batch_size=512
            )
            new_description_embeddings = self.embedding_model.encode(
                new_description,
                device=device,
                show_progress_bar=True,
                batch_size=512
            )
            similarities = _row_cosine_similarity(old_description_embeddings, new_description_embeddings)
            similarities = np.clip(similarities, -1.0, 1.0)
            similarities[~np.array(desc_valid)] = 0.0
            df['description_cosine_similarity'] = similarities

            del old_description_embeddings, new_description_embeddings
            gc.collect()
            torch.cuda.empty_cache()

            # --- Directional NLI entailment scores (old<->new label specificity direction) ---

            o2n_pairs = list(zip(old_label, new_label))
            n2o_pairs = list(zip(new_label, old_label))

            cols_o2n = ['old_to_new_contradiction', 'old_to_new_entailment', 'old_to_new_neutral']
            cols_n2o = ['new_to_old_contradiction', 'new_to_old_entailment', 'new_to_old_neutral']

            if o2n_pairs:
                print("Creating NLI features for entity changes - labels", flush=True)
                o2n_scores = self.nli_model.predict(o2n_pairs, batch_size=128,
                                                    show_progress_bar=True
                                                     )
                n2o_scores = self.nli_model.predict(n2o_pairs, 
                                                    batch_size=128, 
                                                    show_progress_bar=True
                                                    )

                df.loc[df.index, cols_o2n] = np.asarray(o2n_scores)
                df.loc[df.index, cols_n2o] = np.asarray(n2o_scores)
            else:
                df.loc[df.index, cols_o2n] = np.nan
                df.loc[df.index, cols_n2o] = np.nan

            del o2n_pairs, n2o_pairs
            gc.collect()
            torch.cuda.empty_cache()

            o2n_pairs = list(zip(old_description, new_description))
            n2o_pairs = list(zip(new_description, old_description))

            cols_o2n = ['old_to_new_desc_contradiction', 'old_to_new_desc_entailment', 'old_to_new_desc_neutral']
            cols_n2o = ['new_to_old_desc_contradiction', 'new_to_old_desc_entailment', 'new_to_old_desc_neutral']

            if o2n_pairs:
                print("Creating NLI features for entity changes - descriptions", flush=True)
                o2n_scores = self.nli_model.predict(o2n_pairs,
                                                     batch_size=128, 
                                                    show_progress_bar=True
                                                    )
                n2o_scores = self.nli_model.predict(n2o_pairs, 
                                                    batch_size=128, 
                                                    show_progress_bar=True
                                                    )

                df.loc[df.index, cols_o2n] = np.asarray(o2n_scores)
                df.loc[df.index, cols_n2o] = np.asarray(n2o_scores)
            else:
                df.loc[df.index, cols_o2n] = np.nan
                df.loc[df.index, cols_n2o] = np.nan

            del o2n_pairs, n2o_pairs
            gc.collect()
            torch.cuda.empty_cache()

        return df

    @staticmethod
    def _word_diff_breakdown(old_value, new_value):
        
        # keep only letters and numbers, split by whitespace, lowercase
        old_words = set(w.lower() for w in re.findall(r"[a-zA-Z0-9]+", old_value))
        new_words = set(w.lower() for w in re.findall(r"[a-zA-Z0-9]+", new_value))

        # diff words between old and new
        old_not_new = old_words - new_words
        new_not_old = new_words - old_words
        diff_words = old_not_new | new_not_old

        if not diff_words:
            return 0.0, 0.0, 0, 0.0

        # keep stop words that are in the diff
        stopword_diff = (diff_words & STOP_WORDS)

        # remove stop words from the diff to get remaining words
        remaining_old = old_not_new - stopword_diff 
        remaining_new = new_not_old - stopword_diff

        # check if any of the remaining words are plural pairs (e.g., "cat" and "cats")
        plural_pairs = 0
        used_new = set()
        for ow in remaining_old:
            match = next((nw for nw in remaining_new if nw not in used_new and is_plural_pair(ow, nw)), None)
            if match is not None:
                plural_pairs += 1
                used_new.add(match)

        explained = len(stopword_diff) + 2 * plural_pairs

        # count of other words that are not stopwords or plural pairs
        other = max(len(diff_words) - explained, 0)

        #  how many from the differing words are stopwords or plural pairs
        stopword_diff_ratio = len(stopword_diff) / len(diff_words)
        plural_pair_ratio = (2 * plural_pairs) / len(diff_words)

        other_word_ratio = other / len(diff_words)

        return stopword_diff_ratio, plural_pair_ratio, other, other_word_ratio


    @staticmethod
    def create_text_features(datatype, old_value, new_value):
        """Extract features for string & entity changes"""

        new_value = str(new_value).strip().replace('"', '')
        old_value = str(old_value).strip().replace('"', '')

        def calc_overlap(old_value, new_value):
            old_tokens = set(old_value.split())
            new_tokens = set(new_value.split())
            if len(old_tokens | new_tokens) == 0:
                return 0
            return len(old_tokens & new_tokens) / len(old_tokens | new_tokens)

        # percentage (ratio) of token overlap
        token_overlap = calc_overlap(old_value, new_value)

        old_in_new = int(old_value in new_value)
        new_in_old = int(new_value in old_value)

        single_words = (len(old_value.split()) == 1) and (len(new_value.split()) == 1)
        if (token_overlap == 0) and (old_in_new == 0) and (new_in_old == 0):
            if single_words:
                max_len = max(len(old_value), len(new_value)) or 1
                char_dissimilarity = levenshtein_distance(old_value.lower(), new_value.lower()) / max_len
                complete_replacement = int(char_dissimilarity > 0.8)
            else:
                complete_replacement = 1
        else:
            complete_replacement = 0

        result = (
            token_overlap,
            old_in_new,
            new_in_old
        )

        if datatype == 'text':  # remove for entity

            # word-level sequence alignment. This aligns words by
            # position
            word_matcher = difflib.SequenceMatcher(None, old_value.split(), new_value.split())
            word_alignment_ratio = word_matcher.ratio()
            word_inserts = word_deletes = word_replaces = 0
            for tag, i1, i2, j1, j2 in word_matcher.get_opcodes():
                if tag == 'insert':
                    word_inserts += (j2 - j1)
                elif tag == 'delete':
                    word_deletes += (i2 - i1)
                elif tag == 'replace':
                    word_replaces += max(i2 - i1, j2 - j1)
            
            word_insertions = word_inserts
            word_deletions = word_deletes
            word_substitutions = word_replaces

            has_significant_prefix = int(len(os.path.commonprefix([old_value, new_value])) >= 3)
            has_significant_suffix = int(len(os.path.commonprefix([old_value[::-1], new_value[::-1]])) >= 3)

            word_count_old = int(len(old_value.split()))
            word_count_new = int(len(new_value.split()))

            special_char_regex = r'[^a-zA-Z0-9\s]' # exclude white spaces

            special_char_count_old = len(re.findall(special_char_regex, old_value))
            special_char_count_new = len(re.findall(special_char_regex, new_value))

            # special char diff
            special_char_count_diff = special_char_count_old - special_char_count_new

            def _count_accented_chars(text):
                return sum(1 for c in text if strip_accents(c) != c)
            accent_char_count_diff = _count_accented_chars(old_value) - _count_accented_chars(new_value)

            # whitespace count
            whitespace_count_diff = len(re.findall(r'\s', old_value)) - len(re.findall(r'\s', new_value))

            def _count_case_swaps(old_value, new_value):
                """Counts characters that are the same letter but a different case
                between old_value and new_value, via a case-insensitive alignment -
                catches offsetting swaps (e.g. "AaBb" -> "aAbB") that case_diff_count
                (a net uppercase-count difference) would miss entirely, and still
                works when the strings differ in length due to other simultaneous
                edits, since only aligned 'equal' blocks are compared."""

                def _safe_lower_char(c):
                    lc = c.lower()
                    #For the rare chars where lower() expands (e.g.
                    # Turkish 'İ' -> 'i' + combining dot), fall back to the literal
                    # character itself 
                    return lc if len(lc) == 1 else c

                old_words = old_value.split()
                new_words = new_value.split()

                # only compare case within words that are the same ignoring case,
                # i.e. genuinely shared words - not an arbitrary character-level
                # alignment across the whole string
                old_words_lower = [_safe_lower_char(w) for w in old_words]
                new_words_lower = [_safe_lower_char(w) for w in new_words]

                matcher = difflib.SequenceMatcher(None, old_words_lower, new_words_lower)
                swaps = 0
                for tag, i1, i2, j1, j2 in matcher.get_opcodes():
                    if tag == 'equal':
                        for oi, ni in zip(range(i1, i2), range(j1, j2)):
                            old_w, new_w = old_words[oi], new_words[ni]
                            if old_w != new_w and old_w.lower() == new_w.lower():
                                # same word, different case somewhere in it - count char-level swaps within just this word
                                for oc, nc in zip(old_w, new_w):
                                    if oc != nc:
                                        swaps += 1
                return swaps

            # case diff
            case_swap_count = 0 if complete_replacement else _count_case_swaps(old_value, new_value)

            def get_edit_operations(old_value, new_value):
                ops = Levenshtein.editops(old_value, new_value)
                insertions = deletions = substitutions = 0
                for op in ops:
                    """
                        From the DOCS: The result is a list of triples (operation, spos, dpos), where operation is 
                        one of ‘equal’, ‘replace’, ‘insert’, or ‘delete’; spos and dpos are position of characters in
                         the first (source) and the second (destination) strings. These are operations on single characters.
                    """
                    if op[0] == 'insert':
                        insertions += 1
                    elif op[0] == 'delete':
                        deletions += 1
                    elif op[0] == 'replace':
                        substitutions += 1
                return insertions, deletions, substitutions

            char_insertions, char_deletions, char_substitutions = get_edit_operations(old_value, new_value)
            adjacent_char_swap = has_adjacent_swap(old_value, new_value)

            old_len = len(old_value)
            new_len = len(new_value)
            max_len = max(old_len, new_len) if max(old_len, new_len) > 0 else 1
            lev_dist = levenshtein_distance(old_value, new_value)
            # percentage of how much changed (RAW strings)
            raw_edit_distance_ratio = lev_dist / max_len

            # residual edit distance after stripping case/whitespace/special
            # chars/accents - isolates real content change from formatting
            # noise, so it stays > 0 even when a typo co-occurs with a
            # formatting-only change elsewhere in the string.
            old_norm = normalize_for_residual(old_value)
            new_norm = normalize_for_residual(new_value)
            max_norm_len = max(len(old_norm), len(new_norm)) or 1
            residual_edit_distance_ratio = levenshtein_distance(old_norm, new_norm) / max_norm_len

            # stopword/plural decomposition
            stopword_diff_ratio, plural_pair_ratio, other_word_diff_count, other_word_ratio = FeatureCreation._word_diff_breakdown(
                old_value, new_value
            )

            result = result + (

                word_alignment_ratio,

                word_insertions,
                word_deletions,
                word_substitutions,

                has_significant_prefix,
                has_significant_suffix,

                word_count_old,
                word_count_new,

                special_char_count_diff,
                whitespace_count_diff,
                accent_char_count_diff,
                case_swap_count,

                char_insertions,
                char_deletions,
                char_substitutions,

                adjacent_char_swap,

                raw_edit_distance_ratio,
                residual_edit_distance_ratio,

                stopword_diff_ratio,
                plural_pair_ratio,
                other_word_diff_count,
                other_word_ratio
            )

        return result
    

    
