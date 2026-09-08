from openai import OpenAI
import pandas as pd
import time
import os
import re
import json
from transformers import AutoTokenizer
from .const import CLASSES_PER_DATATYPE, CLASS_DESCRIPTION, EXAMPLES_PER_DATATYPE, LLM_RESULTS_DIR
from sklearn.preprocessing import MultiLabelBinarizer
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

class LLMClassifier():
    def __init__(self, config_path: str):

        try:
            with open(config_path, 'r') as f:
                self.config = json.load(f)
        except Exception as e:
            print(f"Error loading LLM classifier configuration from {config_path}: {e}")
            raise e
        
        self.base_url = self.config.get('base_url', '')
        self.api_key = self.config.get('api_key', 'EMPTY')
        self.llm_id = self.config.get('llm_id', '')

        self.client = OpenAI(
            base_url=self.base_url,
            api_key=self.api_key
        )
        model_name = "Qwen/Qwen3.5-35B-A3B-FP8"
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.temperature = self.config.get('temperature', 0)
        self.max_tokens = self.config.get('max_tokens', 100)

    @staticmethod
    def build_context(datatype, examples_text='', batch=False):
        class_description = '\n'.join([f"- {CLASS_DESCRIPTION[datatype][label]}" for label in CLASSES_PER_DATATYPE[datatype]])

        if batch:
            response_format = 'class_1, class_2' if datatype == 'text' else 'class'
            format_instr = (
                f'You will be given several changes, each preceded by its number in brackets, e.g. "[1]".\n'
                f'Answer with exactly one line per change, in the format "[n]: {response_format}"'
                f'(or "[n]: none" if no class applies). Same order as given, nothing else.'
            )
        else:
            response_format = 'names, comma-separated (e.g. "class_1, class_2"' if datatype == 'text' else 'name (e.g., "class")'
            format_instr = f'Answer with ONLY the matching class {response_format}. No explanation.'

        multi_label_text = ''
        if datatype == 'text':
            multi_label_text = 'Evaluate each class independently - a change can match zero, one, or several at once. Do not pick a single "best" class.'
        else:
            multi_label_text = 'Pick the single best class that applies to this change.'

        context = f'''
            You are annotating edits to Wikidata statements. 
            Each edit shows an old_value and a new_value for the same property on the same entity. 
            Your job is to decide which of the following labels apply to the change from old_value to new_value.

            Classes: {CLASSES_PER_DATATYPE[datatype]}
            {class_description}

            {multi_label_text}
            If none clearly apply, say so instead of guessing.

            {format_instr}
            {examples_text}'''
        
        return context

    @staticmethod
    def build_content(data, datatype):
        # Entity Label: {data['entity_label']} \n
        # Property: {data['property_label']} \n
        content = f'''
        Old Value: {data['old_value']} {f"({data['old_value_label']})" if datatype == 'entity' else ''} \n
        New Value: {data['new_value']} {f"({data['new_value_label']})" if datatype == 'entity' else ''} \n
        '''
        if datatype == 'entity':
            content += f"Old value description: {data['old_value_description']} \n"
            content += f"New value description: {data['new_value_description']} \n"

        return content

    @staticmethod
    def build_batch_content(rows, datatype):
        """Formats several changes into one prompt, each numbered so the
        model's response lines can be matched back to the right row."""
        parts = []
        for i, (_, row) in enumerate(rows.iterrows(), start=1):
            parts.append(f"[{i}]\n{LLMClassifier.build_content(row, datatype)}")
        return "\n".join(parts)

    @staticmethod
    def _parse_batch_response(text, rows):
        """Maps '[n]: labels' response lines back to the original
        DataFrame index. Any row the model dropped, merged, or
        mangled comes back as None rather than '' - the caller should
        treat None as 'needs an individual fallback call', not as a
        genuine empty label."""
        pattern = re.compile(r'^\[(\d+)\]\s*:\s*(.*)$')
        parsed = {}
        for line in text.splitlines():
            m = pattern.match(line.strip())
            if not m:
                continue
            n = int(m.group(1))
            label = m.group(2).strip()
            parsed[n] = label

        results = {}
        for i, (idx, _) in enumerate(rows.iterrows(), start=1):
            results[idx] = parsed.get(i, None)

        return results

    def classify_batch(self, rows, context, datatype):
        """
        rows: a DataFrame slice (the batch).
        Returns a dict {original_index: label_string_or_None}. None
        means the model's response didn't include a parseable line
        for that row - the caller is responsible for falling back to
        an individual classify() call for those.
        """
        content = self.build_batch_content(rows, datatype)
        # tokens = self.tokenizer.tokenize(content)
        # token_count = len(tokens)
        # print(f"Batch content:\n{content}")
        # print(f"Total tokens of content: {token_count}")
        prompt = [
            {"role": "system", "content": context},
            {"role": "user", "content": content},
        ]
        try:
            response = self.client.chat.completions.create(
                model=self.llm_id,
                messages=prompt,
                temperature=self.temperature,
                max_tokens=self.max_tokens,
                top_p=0.8,
                extra_body={
                    "top_k": 20,
                    "chat_template_kwargs": {"enable_thinking": False}
                }
            )
            text = response.choices[0].message.content.strip()
            print('Finish reason:', response.choices[0].finish_reason)
            # tokens = self.tokenizer.tokenize(text)
            # token_count = len(tokens)
            # print(f"Batch response tokens: {token_count}")
            # print(f"Batch response:\n{text}")
        except Exception as e:
            print(f"Error making batch request: {e}")
            text = ''

        return self._parse_batch_response(text, rows)

    def classify(self, change, context, datatype):
        """ 
            Sends request to classify a single change to the LLM and returns the predicted class.
        """

        prompt = [
            {"role": "system", 
            "content": context}, 
            {'role': 'user',
            'content': LLMClassifier.build_content(change, datatype)}
        ]
        try:

            # Instruct (or non-thinking) mode for general tasks:
            # temperature=0.7, top_p=0.8, top_k=20, min_p=0.0, presence_penalty=1.5, repetition_penalty=1.0

            response = self.client.chat.completions.create(
                model=self.llm_id,
                messages=prompt,
                temperature=self.temperature,
                max_tokens=self.max_tokens,
                top_p=0.8,
                extra_body={
                    "top_k": 20,
                    "chat_template_kwargs": {"enable_thinking": False}
                }
            )
            result = response.choices[0].message.content.strip()
            return result
        
        except Exception as e:
            print(f"Error making request: {e}")
            return ''
    
    def generate_examples_text(self, datatype):
        """Returns a string of example changes for each class, to be
        included in the prompt context. Each example is formatted like
        a normal change, so the model sees the same structure as the
        actual classification task."""
        examples = EXAMPLES_PER_DATATYPE[datatype]
        examples_str = []

        for label, examples_label in examples.items():
            for example in examples_label:
                # entity_label = example[0]
                # property_label = example[1]
                if datatype == 'entity':
                    old_value = example[0]
                    new_value = example[1]
                else:
                    old_value = example[2]
                    new_value = example[3]
                # Entity Label: {entity_label}\nProperty: {property_label}\n
                examples_str.append(f"Old Value: {old_value}\nNew Value: {new_value}\nOutput: {label}")

        return "\n".join(examples_str)

    def _run_batch_classification(self, df, datatype, output_col, batch_size=5, examples_text=''):
        """Chunks df into batches, calls classify_batch per chunk, and
        falls back to a single classify() call for any row the batch
        response didn't cover."""

        examples_text = self.generate_examples_text(datatype)
        batch_context = self.build_context(datatype, examples_text, batch=True)
        # tokens = self.tokenizer.tokenize(batch_context)
        # token_count = len(tokens)
        single_context = batch_context

        df[output_col] = ''
        start_time = time.time()
        for start in range(0, len(df), batch_size):
            chunk = df.iloc[start:start + batch_size]
            results = self.classify_batch(chunk, batch_context, datatype)
            for idx, label in results.items():
                if label is None or label == '':
                    label = self.classify(df.loc[idx], single_context, datatype)
                df.at[idx, output_col] = label
        end_time = time.time()
        with open(f'{LLM_RESULTS_DIR}/llm_classification_time.txt', 'w') as f:
            f.write(f"Time taken for LLM classification of {datatype} changes: {end_time - start_time} seconds\n")
        return df
    

    def evaluate(self):
        results = {datatype: {} for datatype in CLASSES_PER_DATATYPE.keys()}
        datatypes = list(CLASSES_PER_DATATYPE.keys())
        for datatype in datatypes:
            if os.path.exists(f'{LLM_RESULTS_DIR}/gs_{datatype}_with_llm_labels.csv'):
                df = pd.read_csv(f'{LLM_RESULTS_DIR}/gs_{datatype}_with_llm_labels.csv')

                def parse_labels(val):
                    if pd.isna(val) or str(val).strip() == '':
                        return []
                    return [l.strip() for l in str(val).split(',')]

                df['labels_list'] = df['label'].apply(parse_labels)
                df['llm_label_list'] = df['llm_label'].apply(parse_labels)

                label_binarizer = MultiLabelBinarizer()
                y_true_binary = label_binarizer.fit_transform(df['labels_list'])
                y_pred_binary = label_binarizer.transform(df['llm_label_list'])

                for i, label in enumerate(label_binarizer.classes_):
                    y_true = y_true_binary[:, i]
                    y_pred = y_pred_binary[:, i]

                    accuracy = accuracy_score(y_true, y_pred)
                    precision = precision_score(y_true, y_pred, zero_division=0)
                    recall = recall_score(y_true, y_pred, zero_division=0)
                    f1 = f1_score(y_true, y_pred, zero_division=0)

                    results[datatype][label] = {
                        'precision': precision,
                        'recall': recall,
                        'accuracy': accuracy,
                        'f1': f1
                    }

        with open(f'{LLM_RESULTS_DIR}/llm_classification.json', 'w') as f:
            json.dump(results, f, indent=4)
    
        return results
