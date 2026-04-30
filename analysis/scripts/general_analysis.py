import pandas as pd
import matplotlib.pyplot as plt
import psycopg2
from pathlib import Path
import json
import yaml
import os
import numpy as np
import matplotlib as mpl
import textwrap

from analysis.scripts.utils import execute_query, query_to_df

SQL_SCRIPT_DIR = 'analysis/sql'
RESULTS_DIR = 'analysis/results'

os.makedirs(RESULTS_DIR, exist_ok=True)

RESULTS_DIR_FIGURES = f'{RESULTS_DIR}/figures'
os.makedirs(RESULTS_DIR_FIGURES, exist_ok=True)

# https://jrnold.github.io/ggthemes/reference/ptol_pal.html
color_palette = [
    '#4477AA', '#88CCEE', '#117733', '#DDCC77', '#CC6677', 
    '#AA4499', '#332288', '#6699CC', '#44AA99', 
    '#117733', '#999933', '#661100', '#882255' 
]

clear_color_palette = ['#88CCEE', '#DDCC77', '#CC6677', '#44AA99', '#999933', '#6699CC']

def format_number(n):
    n = float(n)
    if n >= 1_000_000_000:
        return f'{n/1_000_000_000:.1f}B'
    elif n >= 1_000_000:
        v = n / 1_000_000
        return f'{v:.1f}M'
    elif n >= 1_000:
        v = n / 1_000
        return f'{v:.1f}K'
    return str(int(n))

  
def save_fig(fig, path):
    plt.tight_layout()
    fig.savefig(path, dpi=300, bbox_inches='tight', pad_inches=0)
    plt.close(fig)


# Descriptive analysis of dataset
def property_stats(db_config, reload_data):

    query_name = 'stats_properties'
    if reload_data:
        conn = psycopg2.connect(**db_config)
        with open(f'{SQL_SCRIPT_DIR}/{query_name}.sql', 'r') as f:
            sql_query = f.read()
        execute_query(conn, sql_query)
        df = query_to_df(conn, 'SELECT * FROM stats_properties;')
        df.to_csv(f'{RESULTS_DIR}/stats_properties.csv', index=False)
    else:
        if os.path.exists(f'{RESULTS_DIR}/stats_properties.csv'):
            df = pd.read_csv(f'{RESULTS_DIR}/stats_properties.csv')
        else:
            conn = psycopg2.connect(**db_config)
            df = query_to_df(conn, 'SELECT * FROM stats_properties;')
            if len(df) == 0:
                print(f'No results found for stats_properties. Please run with reload_data=True (see setup.yml) to execute the query and save results.')
                return
            df.to_csv(f'{RESULTS_DIR}/stats_properties.csv', index=False)

    top_filter = 10

    df['property_label'] = df['property_label'].fillna('Label Unknown')
    df['display_label'] = df['property_label'].apply(
        lambda x: '\n'.join(textwrap.wrap(str(x), width=29))
    )
    df_filtered = df[df['count_entities'] >= 100].copy()

    df_filtered['pct_create'] = (df_filtered['count_create'] / df_filtered['count_changes']) * 100
    df_filtered['pct_delete'] = (df_filtered['count_delete'] / df_filtered['count_changes']) * 100
    df_filtered['pct_update'] = (df_filtered['count_update'] / df_filtered['count_changes']) * 100

    mpl.rcParams.update({
        'font.size': 4,
        'axes.titlesize': 4,
        'axes.labelsize': 4,
        'xtick.labelsize': 4,
        'ytick.labelsize': 4,
        'legend.fontsize': 4,
        'figure.dpi': 300,
        'axes.spines.top': False,
        'axes.spines.right': False,
        'axes.grid': False,
    })

    # --- Plot 1: Top properties most used ---
    df_filtered.sort_values('count_entities', ascending=True, inplace=True)  # ascending for barh so top is at top
    df_top = df_filtered.tail(top_filter) # get tail

    fig, ax = plt.subplots(figsize=(2.5, 2))
    bars = ax.barh(df_top['display_label'], df_top['count_entities'], color=clear_color_palette[2], edgecolor='none', height=0.5)
    ax.bar_label(bars, labels=[format_number(c) for c in df_top['count_entities']], fontsize=4)
    ax.set_xlabel('Number of Entities')
    # ax.set_title(f'Top {top_filter} Entity Types by Number of Enities')
    ax.xaxis.set_major_formatter(mpl.ticker.FuncFormatter(lambda x, _: format_number(x)))
    save_fig(fig, f'{RESULTS_DIR_FIGURES}/property_count_{top_filter}.png')

    # --- Plot 2: Type of change stacked bar ---
    df_top = df_filtered.nlargest(top_filter, 'count_entities')
    x = np.arange(len(df_top))

    fig, ax = plt.subplots(figsize=(2.5, 2))
    ax.bar(x, df_top['pct_create'], label='Create', color=clear_color_palette[0], edgecolor='none')
    ax.bar(x, df_top['pct_delete'], bottom=df_top['pct_create'], label='Delete', color=clear_color_palette[1], edgecolor='none')
    ax.bar(x, df_top['pct_update'], bottom=df_top['pct_create'] + df_top['pct_delete'], label='Update', color=clear_color_palette[2], edgecolor='none')
    ax.set_xticks(x)
    ax.set_xticklabels(df_top['display_label'], rotation=45, ha='right', fontsize=4)
    ax.set_ylabel('Percentage of changes')
    fig.legend(loc='outside lower right', markerscale=0.2, handlelength=1)
    save_fig(fig, f'{RESULTS_DIR}/figures/property_top_{top_filter}_change_type.png')


def entity_type_stats(db_config, reload_data):

    query_name = 'stats_entity_type'
    if reload_data:
        conn = psycopg2.connect(**db_config)
        with open(f'{SQL_SCRIPT_DIR}/{query_name}.sql', 'r') as f:
            sql_query = f.read()
        execute_query(conn, sql_query)
        df = query_to_df(conn, 'SELECT * FROM entity_type_stats;')
        df.to_csv(f'{RESULTS_DIR}/entity_type_stats.csv', index=False)
    else:
        if os.path.exists(f'{RESULTS_DIR}/entity_type_stats.csv'):
            df = pd.read_csv(f'{RESULTS_DIR}/entity_type_stats.csv')
        else:
            conn = psycopg2.connect(**db_config)
            df = query_to_df(conn, 'SELECT * FROM entity_type_stats;')
            if len(df) == 0:
                print(f'No results found for entity_type_stats. Please run with reload_data=True (see setup.yml) to execute the query and save results.')
                return
            df.to_csv(f'{RESULTS_DIR}/entity_type_stats.csv', index=False)

    top_filter = 10

    # sandbox entities
    entities_to_filter = ['Q16943273', 'Q17339402', 'Q4115189', 'Q13406268', 'Q15397819', 'Q112795079']
    df = df[~df['individual_type'].isin(entities_to_filter)].copy()

    # count is count of entities
    df.sort_values(by='count', ascending=False, inplace=True)
    df['entity_type_label'] = df['entity_type_label'].fillna('Label Unknown')

    label_counts = df['entity_type_label'].value_counts()
    df['display_label'] = df.apply(
        lambda row: f"{row['entity_type_label']} ({row['individual_type']})" 
        if label_counts[row['entity_type_label']] > 1 
        else row['entity_type_label'], 
        axis=1
    )

    df['display_label'] = df['display_label'].apply(
        lambda x: '\n'.join(textwrap.wrap(str(x), width=25))
    )

    df['total_edits_by_users'] = df['num_bot_edits'] + df['num_anonymous_edits'] + df['registered_user_edits']
    df['pct_bot'] = (df['num_bot_edits'] / df['total_edits_by_users']) * 100
    df['pct_anonymous'] = (df['num_anonymous_edits'] / df['total_edits_by_users']) * 100
    df['pct_registered'] = (df['registered_user_edits'] / df['total_edits_by_users']) * 100

    mpl.rcParams.update({
        'font.size': 4,
        'axes.titlesize': 4,
        'axes.labelsize': 4,
        'xtick.labelsize': 4,
        'ytick.labelsize': 4,
        'legend.fontsize': 4,
        'figure.dpi': 300,
        'axes.spines.top': False,
        'axes.spines.right': False,
        'axes.grid': False,
    })

    # --- Plot 1: Top entity types by count ---
    df.sort_values('count', ascending=True, inplace=True)  # ascending for barh so top is at top
    df_top = df.tail(top_filter)

    fig, ax = plt.subplots(figsize=(2.5, 2))
    bars = ax.barh(df_top['display_label'], df_top['count'], color=clear_color_palette[2], edgecolor='none')
    ax.bar_label(bars, labels=[format_number(c) for c in df_top['count']], fontsize=4)
    ax.set_xlabel('Number of Entities')
    # ax.set_title(f'Top {top_filter} Entity Types by Number of Enities')
    ax.xaxis.set_major_formatter(mpl.ticker.FuncFormatter(lambda x, _: format_number(x)))
    save_fig(fig, f'{RESULTS_DIR}/figures/entity_type_count_{top_filter}.png')

    # --- Plot 2: Top by value changes ---
    
    df.sort_values('num_value_changes', ascending=True, inplace=True)
    df_top = df.tail(top_filter)

    fig, ax = plt.subplots(figsize=(2.5, 2))
    
    bars = ax.barh(df_top['display_label'], df_top['num_value_changes'], color=clear_color_palette[0], edgecolor='none')
    ax.bar_label(bars, labels=[format_number(c) for c in df_top['num_value_changes']])
    ax.set_xlabel('Number of Value Changes')
    # ax.set_title(f'Top {top_filter} Entity Types by Value Changes')
    ax.xaxis.set_major_formatter(mpl.ticker.FuncFormatter(lambda x, _: format_number(x)))

    save_fig(fig, f'{RESULTS_DIR}/figures/entity_type_top_{top_filter}_value_changes.png')

    fig, ax = plt.subplots(figsize=(2.5, 2))
    
    df['value_changes_per_entity'] = df['num_value_changes'] / df['count'] # normalize by count so I don't just get the
    df_filtered = df[df['count'] >= 100].copy() # filter out the ones with very low count, if not the ratio is skewed
    df_filtered.sort_values('value_changes_per_entity', ascending=True, inplace=True)
    df_top = df_filtered.tail(top_filter)

    bars = ax.barh(df_top['display_label'], df_top['value_changes_per_entity'], color=clear_color_palette[1], edgecolor='none')
    ax.bar_label(bars, labels=[format_number(c) for c in df_top['value_changes_per_entity']])
    ax.set_xlabel('Avg. Number of Value Changes per Entity of the Type')
    # ax.set_title(f'Top {top_filter} Most Edited Entity Types')
    ax.xaxis.set_major_formatter(mpl.ticker.FuncFormatter(lambda x, _: format_number(x)))

    plt.subplots_adjust(wspace=0.5)

    save_fig(fig, f'{RESULTS_DIR}/figures/entity_type_top_{top_filter}_most_edited.png')

    # --- Plot 3: 10 less edited entity types by value changes ---

    df_top = df.head(top_filter) # get the head for the less edited ones because it's ascending true

    fig, ax = plt.subplots(figsize=(2.5, 2))
    
    df['value_changes_per_entity'] = df['num_value_changes'] / df['count'] # normalize by count so I don't just get the
    df_filtered = df[df['count'] >= 100].copy() # filter out the ones with very low count, if not the ratio is skewed
    df_filtered.sort_values('value_changes_per_entity', ascending=True, inplace=True)
    df_top = df.head(top_filter)
    print(df_top[['display_label', 'individual_type', 'value_changes_per_entity']])

    bars = ax.barh(df_top['display_label'], df_top['value_changes_per_entity'], color=clear_color_palette[1], edgecolor='none')
    ax.bar_label(bars, labels=[format_number(c) for c in df_top['value_changes_per_entity']])
    ax.set_xlabel('Avg. Number of Value Changes per Entity of the Type')
    # ax.set_title(f'Top {top_filter} Most Edited Entity Types')
    ax.xaxis.set_major_formatter(mpl.ticker.FuncFormatter(lambda x, _: format_number(x)))

    plt.subplots_adjust(wspace=0.5)

    save_fig(fig, f'{RESULTS_DIR}/figures/entity_type_top_{top_filter}_less_edited.png')


    # --- Plot 4: User type stacked bar ---
    df_top = df_filtered.nlargest(top_filter, 'value_changes_per_entity')
    x = np.arange(len(df_top))

    fig, ax = plt.subplots(figsize=(2.5, 2))
    ax.bar(x, df_top['pct_bot'], label='Bot', color=clear_color_palette[0], edgecolor='none')
    ax.bar(x, df_top['pct_anonymous'], bottom=df_top['pct_bot'], label='Anonymous', color=clear_color_palette[1], edgecolor='none')
    ax.bar(x, df_top['pct_registered'], bottom=df_top['pct_bot'] + df_top['pct_anonymous'], label='Registered', color=clear_color_palette[2], edgecolor='none')
    ax.set_xticks(x)
    ax.set_xticklabels(df_top['display_label'], rotation=45, ha='right', fontsize=4)
    ax.set_ylabel('Percentage of Edits (%)')
    # ax.set_title(f'Edit Distribution by User Type (Top {top_filter} Most Edited Entity Types)')
    fig.legend(loc='outside lower right', markerscale=0.2, handlelength=1)
    save_fig(fig, f'{RESULTS_DIR}/figures/entity_type_top_{top_filter}_user_type.png')


def distribution_of_revisions_value_changes(db_config, reload_data):

    if reload_data:
        conn = psycopg2.connect(**db_config)
        df = query_to_df(conn, 'SELECT entity_id, num_revisions, num_value_changes FROM entity_stats;')
        df.to_csv(f'{RESULTS_DIR}/entity_stats.csv', index=False)
    else:
        if os.path.exists(f'{RESULTS_DIR}/entity_stats.csv'):
            df = pd.read_csv(f'{RESULTS_DIR}/entity_stats.csv')
        else:
            conn = psycopg2.connect(**db_config)
            df = query_to_df(conn, 'SELECT entity_id, num_revisions, num_value_changes FROM entity_stats;')
            if len(df) == 0:
                print(f'No results found for entity_stats. Please run with reload_data=True (see setup.yml) to execute the query and save results.')
                return
            df.to_csv(f'{RESULTS_DIR}/entity_stats.csv', index=False)

    mpl.rcParams.update({
        'font.size': 3,
        'axes.titlesize': 4,
        'axes.labelsize': 5,
        'xtick.labelsize': 5,
        'ytick.labelsize': 5,
        'legend.fontsize': 5,
        'figure.dpi': 300,
        'axes.spines.top': False,
        'axes.spines.right': False,
    })

    fig, axes = plt.subplots(1, 2, figsize=(2, 2))
    df = df[df['entity_id'] != '4115189']

    # 2. Distribution of revisions per entity (histogram)
    axes[0].hist(df['num_revisions'], bins=20, color=clear_color_palette[0], edgecolor='none')
    axes[0].set_yscale('log')
    # axes[0].set_title('Distribution of Revisions per Entity')
    axes[0].set_xlabel('Number of Revisions')
    axes[0].set_ylabel('Number of Entities (log)')

    # 4. Distribution of value changes per entity
    axes[1].hist(df['num_value_changes'], bins=20, color=clear_color_palette[1], edgecolor='none')
    axes[1].set_yscale('log')
    # axes[1].set_title('Distribution of Value Changes per Entity')
    axes[1].set_xlabel('Number of Value\nChanges')
    axes[1].set_ylabel('Number of Entities (log)')

    plt.tight_layout()
    fig.savefig(f'{RESULTS_DIR}/figures/distribution_revisions_value_changes.png', dpi=300, bbox_inches='tight')
    plt.close(fig)
    
    max_revisions = df['num_revisions'].max()
    max_value_changes = df['num_value_changes'].max()
    min_revisions = df['num_revisions'].min()
    min_value_changes = df['num_value_changes'].min()
    avg_value_changes = df['num_value_changes'].mean()
    avg_num_revisions = df['num_revisions'].mean()

    entity_with_most_revisions = df.loc[df['num_revisions'].idxmax()]['entity_id']
    entity_with_most_value_changes = df.loc[df['num_value_changes'].idxmax()]['entity_id']

    print('================ STATISTICS ================')
    print(f'Max number of revisions for an entity: {max_revisions}, Min number of revisions for an entity: {min_revisions}')
    print(f'Max number of value changes for an entity: {max_value_changes}, Min number of value changes for an entity: {min_value_changes}')
    print(f'Entity with most revisions: {entity_with_most_revisions} ({max_revisions} revisions)')
    print(f'Entity with most value changes: {entity_with_most_value_changes} ({max_value_changes} value changes)')
    
    print(f'Average number of value changes per entity: {avg_value_changes:.2f}')
    print(f'Average number of revisions per entity: {avg_num_revisions:.2f}')

def entity_stats(db_config, reload_data, filter_big_entities):
    """
        Entity stats analysis: distributions of number of revisions, creates, deletes, updates per entity.
    """

    if reload_data:
        conn = psycopg2.connect(**db_config)
        df_final = pd.DataFrame()
        batch_size = 100000
        offset = 0
        while True:
            query = """
                SELECT * 
                FROM entity_stats
                OFFSET {offset} LIMIT {limit}
            """.format(offset=offset, limit=batch_size)

            offset += batch_size
            df = query_to_df(conn, query)
            print(f'Fetched {len(df)} records from offset {offset - batch_size}')
            if len(df) == 0:
                break
                
            df_final = pd.concat([df_final, df], ignore_index=True)

        df_final.to_csv(f'{RESULTS_DIR}/entity_stats_full.csv', index=False)
    else:
        df_final = pd.read_csv(f'{RESULTS_DIR}/entity_stats_full.csv')

    if filter_big_entities:
        # -- earth > 20.000
        # -- sandbox > 60.000
        df_filtered = df_final[df_final['num_revisions'] < 60000]
        print(f'Filtered out big entities: {len(df_final) - len(df_filtered)}')
    else:
        df_filtered = df_final

    fig, axes = plt.subplots(4, 2, figsize=(14, 10))
    font_size = 3

    # histogram of revisions per entity
    bars = axes[0, 0].hist(df_filtered['num_revisions'], bins=50, edgecolor='black', alpha=0.7) # returns values, bins, bars
    axes[0, 0].set_xlabel('Number of Revisions')
    axes[0, 0].set_ylabel('Number of Entities')
    axes[0, 0].set_title('Distribution of Revisions per Entity')
    axes[0, 0].bar_label(bars[2], fontsize=5, color='black', padding=3)
    axes[0, 0].set_yscale('log') 

    # histogram of creates
    bars = axes[0, 1].hist(df_filtered['num_value_change_creates'], bins=50, edgecolor='black', alpha=0.7, color='green')
    axes[0, 1].set_xlabel('Number of Creates')
    axes[0, 1].set_ylabel('Number of Entities')
    axes[0, 1].set_title('Distribution of Creates per Entity')
    axes[0, 1].bar_label(bars[2], fontsize=font_size, color='black', padding=3)
    axes[0, 1].set_yscale('log') 

    # histogram of deletes
    bars = axes[1, 0].hist(df_filtered['num_value_change_deletes'], bins=50, edgecolor='black', alpha=0.7, color='red')
    axes[1, 0].set_xlabel('Number of Deletes')
    axes[1, 0].set_ylabel('Number of Entities')
    axes[1, 0].set_title('Distribution of Deletes per Entity')
    axes[1, 0].bar_label(bars[2], fontsize=font_size, color='black', padding=3)
    axes[1, 0].set_yscale('log') 

    # histogram of updates
    bars = axes[1, 1].hist(df_filtered['num_value_change_updates'], bins=50, edgecolor='black', alpha=0.7, color='orange')
    axes[1, 1].set_xlabel('Number of Updates')
    axes[1, 1].set_ylabel('Number of Entities')
    axes[1, 1].set_title('Distribution of Updates per Entity')
    axes[1, 1].bar_label(bars[2], fontsize=font_size, color='black', padding=3)
    axes[1, 1].set_yscale('log') 

    plt.tight_layout()
    if filter_big_entities:
        img_file_name = f'{RESULTS_DIR}/figures/entity_stats_distributions.png'
    else:
        img_file_name = f'{RESULTS_DIR}/figures/entity_stats_distributions_no_filter.png'
    plt.savefig(img_file_name, dpi=300)
    plt.show()

def stats_sa_ao(db_config, reload_data):

    query_name = 'stats_sa_ao'
    suffixes = ['_ao', '_sa']
    if reload_data:
        conn = psycopg2.connect(**db_config)
        for suffix in suffixes:
            
            with open(f'{SQL_SCRIPT_DIR}/{query_name}.sql', 'r') as f:
                sql_query = f.read()
                sql_query = sql_query.replace('<suffix>', suffix)
            
            df = query_to_df(conn, sql_query)
            print(df.style.format({"count": "{:,.0f}"}).to_string())
            df.to_csv(f'{RESULTS_DIR}/stats{suffix}.csv', index=False)
    else:

        for suffix in suffixes:
            df = pd.read_csv(f'{RESULTS_DIR}/stats{suffix}.csv')
            print(df.style.format({"count": "{:,.0f}"}).to_string())

if __name__ == "__main__":

    root_dir = Path(__file__).parent.parent.parent

    set_up_path = root_dir / Path('setup.yml')
    with open(set_up_path, 'r') as f:
        set_up = yaml.safe_load(f)

    with open(set_up['database_config_path'], 'r') as f:
        db_config = json.load(f)

    # -----------------------------------------------------------------
    # Most edited entity types
    # -----------------------------------------------------------------
    entity_types_setup = set_up['analysis']['entity_types_analysis']
    if entity_types_setup['execute']:
        entity_type_stats(db_config, entity_types_setup['reload_data'])

    # -----------------------------------------------------------------
    # Distribution of revisions and value changes across all entities
    # -----------------------------------------------------------------
    distribution_of_revisions_value_changes_setup = set_up['analysis']['distribution_of_revisions_value_changes']
    if distribution_of_revisions_value_changes_setup['execute']:
        distribution_of_revisions_value_changes(db_config, distribution_of_revisions_value_changes_setup['reload_data'])

    # -----------------------------------------------------------------
    # Most used properties + distribution of user types
    # -----------------------------------------------------------------
    property_stats_setup = set_up['analysis']['property_stats']
    if property_stats_setup['execute']:
        property_stats(db_config, property_stats_setup['reload_data'])

    # -----------------------------------------------------------------
    # Different stats about entities: distribution of revisions, creates, deletes, updates per entity.
    # -----------------------------------------------------------------
    entity_stats_setup = set_up['analysis']['entity_stats']
    if entity_stats_setup['execute']:
        entity_stats(db_config, entity_stats_setup['reload_data'], entity_stats_setup['filter_big_entities'])

    # -----------------------------------------------------------------
    # Different stats about scholarly articles and astronomical objects.
    # -----------------------------------------------------------------
    stats_sa_ao_setup = set_up['analysis']['stats_sa_ao']
    if stats_sa_ao_setup['execute']:
        stats_sa_ao(db_config, stats_sa_ao_setup['reload_data'])