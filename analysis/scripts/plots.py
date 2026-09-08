import os
import sys
from io import StringIO

import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import Patch

DATA_DIR = 'data'
RESULTS_DIR = 'results'
FIGURES_DIR = f'{RESULTS_DIR}/figures'

# Reconstructed Paul Tol "muted" colorblind-safe qualitative palette
# (https://sronpersonalpages.nl/~pault/) - contains the 2 colors the user
# remembered (teal #44AA99, rose #CC6677) plus the rest of that palette.
TOL_MUTED = {
    'indigo': '#332288',
    'cyan': '#88CCEE',
    'teal': '#44AA99',
    'green': '#117733',
    'olive': '#999933',
    'sand': '#DDCC77',
    'rose': '#CC6677',
    'wine': '#882255',
    'purple': '#AA4499',
    'grey': '#DDDDDD',
}

COLOR_NON_REVERTED = TOL_MUTED['teal']
COLOR_REVERTED = TOL_MUTED['rose']

# Additional accent colors the user already uses elsewhere, reused here for
# the rule-based vs. ml-based split.
COLOR_RULE_BASED = '#6699CC'
COLOR_ML_BASED = '#664400'

DISPLAY_LABELS = {
    'qualifier_insertion': 'qualifier insertion',
    'qualifier_deletion': 'qualifier deletion',
    'reference_insertion': 'reference insertion',
    'reference_deletion': 'reference deletion',
    'statement_insertion': 'statement insertion',
    'statement_deletion': 'statement deletion',
    'refinement': 'refinement',
    'unrefinement': 'unrefinement',
    'textual_change': 'textual change',
    'property_value_update': 'value update',
    're_formatting': 're-formatting',
    'soft_insertion': 'soft insertion',
    'soft_deletion': 'soft deletion',
}


def read_csv_robust(path):
    # Some exported CSVs have a stray trailing comma on a row (extra empty
    # field), which trips up the C parser. Strip trailing commas per-line.
    with open(path) as f:
        lines = [line.rstrip('\n').rstrip(',') for line in f]
    return pd.read_csv(StringIO('\n'.join(lines)))


def load_csv(name):
    path = f'{DATA_DIR}/{name}.csv'
    if not os.path.exists(path):
        print(f'File {path} not found. Please run the SQL query first inside sql/ folder.')
        sys.exit(1)
    return read_csv_robust(path)


def format_number(n):
    n = float(n)
    for suffix, divisor in [('B', 1_000_000_000), ('M', 1_000_000), ('K', 1_000)]:
        if abs(n) >= divisor:
            num_to_display = round(n / divisor, 1)
            if num_to_display.is_integer():
                return f'{int(num_to_display)}{suffix}'
            else:
                return f'{num_to_display:.1f}{suffix}'
    return f'{int(n)}'


def parse_triplet(df, base_label, label_col='label', count_col='count'):
    """Look up total/reverted/non_reverted for `base_label` in a long
    label/count dataframe where reverted and non-reverted are their own
    rows named f'{base_label}_reverted' / f'{base_label}_non_reverted'.
    Returns (total, reverted, non_reverted); the latter two are None when
    no such rows exist (revert data unavailable for that label)."""
    lookup = dict(zip(df[label_col], df[count_col]))
    total = lookup.get(base_label)
    reverted = lookup.get(f'{base_label}_reverted')
    non_reverted = lookup.get(f'{base_label}_non_reverted')
    return total, reverted, non_reverted


# -----------------------------------------------------------------------
# Plot 1: change type distribution with revert percentage
# -----------------------------------------------------------------------

def load_change_type_distribution():
    qualifier = load_csv('qualifier_change_stats')
    reference = load_csv('reference_change_stats')
    rank = load_csv('rank_change_stats')
    value = load_csv('value_change_stats')
    entity = load_csv('updates_entity_rev_stats')
    text = load_csv('updates_text_rev_stats')

    for df, col in [(qualifier, 'count'), (reference, 'count'), (rank, 'count'), (value, 'count')]:
        df[col] = pd.to_numeric(df[col])
    for col in ['total_counts', 'reverted', 'non_reverted']:
        entity[col] = pd.to_numeric(entity[col])
    for col in ['count_updates', 'reverted', 'non_reverted']:
        text[col] = pd.to_numeric(text[col])

    rows = []

    # qualifier/reference insertion & deletion: no revert data is tracked
    # for these in the source tables.
    for src_df, base in [
        (qualifier, 'qualifier_insertion'),
        (qualifier, 'qualifier_deletion'),
        (reference, 'reference_insertion'),
        (reference, 'reference_deletion'),
    ]:
        total, reverted, non_reverted = parse_triplet(src_df, base)
        rows.append(dict(key=base, total=total, reverted=reverted, non_reverted=non_reverted))

    # statement insertion/deletion: revert data available from value_change_stats
    for base in ['statement_insertion', 'statement_deletion']:
        total, reverted, non_reverted = parse_triplet(value, base)
        rows.append(dict(key=base, total=total, reverted=reverted, non_reverted=non_reverted))

    # soft insertion / soft deletion: from rank_change_stats and qualifier_change_stats (soft deletion can be a qualifier insertin)
    for base, key in [('count_soft_insertion', 'soft_insertion'), ('count_soft_deletion', 'soft_deletion')]:
        total, reverted, non_reverted = parse_triplet(rank, base)
        if key == 'soft_deletion':
            # qualifier soft deletions have no revert tracking, so their
            # count can only be folded into non_reverted (unknown revert
            # status) to keep total == reverted + non_reverted for the bar.
            total_qual, _, _ = parse_triplet(qualifier, key)
            total += total_qual
            non_reverted += total_qual
        rows.append(dict(key=key, total=total, reverted=reverted, non_reverted=non_reverted))

    # re-formatting: only classified via the rule-based
    total, reverted, non_reverted = parse_triplet(value, 'count_rb_re_formatting')
    rows.append(dict(key='re_formatting', total=total, reverted=reverted, non_reverted=non_reverted))

    # refinement / unrefinement / textual_change / value update: these are
    # also present as count_rb_* rows in value_change_stats
    for base in ['property_value_update', 'refinement', 'unrefinement', 'textual_change']:
        total_rb, reverted_rb, non_reverted_rb = parse_triplet(value, f'count_rb_{base}')
        entity_rows = entity[entity['label'] == base]
        text_rows = text[text['individual_label'] == base]
        total = entity_rows['total_counts'].sum() + text_rows['count_updates'].sum() + total_rb
        reverted = entity_rows['reverted'].sum() + text_rows['reverted'].sum() + reverted_rb
        non_reverted = entity_rows['non_reverted'].sum() + text_rows['non_reverted'].sum() + non_reverted_rb
        rows.append(dict(key=base, total=total, reverted=reverted, non_reverted=non_reverted))

    df = pd.DataFrame(rows)
    df['label'] = df['key'].map(DISPLAY_LABELS)
    
    soft_deletion_qual = parse_triplet(qualifier, 'soft_deletion')[0]
    total_number_of_changes_class = df['total'].sum() 
    total_non_soft_deletion_qual = total_number_of_changes_class - soft_deletion_qual
    print(f'Total number of changes in each class: {total_number_of_changes_class}')
    print(f'Total number of changes in each class (excluding soft deletion from qualifier since they are CREATE qualifier edit events): {total_non_soft_deletion_qual}')
    return df


def plot_change_type_distribution(df, out_path):
    df = df.sort_values('total').reset_index(drop=True)
    y = list(range(len(df)))

    has_revert_data = df['reverted'].notna()
    non_reverted_vals = df['non_reverted'].where(has_revert_data, df['total'])
    reverted_vals = df['reverted'].fillna(0)

    fig, ax = plt.subplots(figsize=(7, 6))
    ax.barh(y, non_reverted_vals, color=COLOR_NON_REVERTED, edgecolor='black', linewidth=0.5, label='non-reverted')
    ax.barh(y, reverted_vals, left=non_reverted_vals, color=COLOR_REVERTED, edgecolor='black', linewidth=0.5, label='reverted')

    for i, row in df.iterrows():
        total = row['total']
        print(f"Label: {row['label']}, Total: {total}, Reverted: {row['reverted']}, Non-reverted: {row['non_reverted']}")
        if pd.notna(row['reverted']):
            pct = row['reverted'] / total * 100
            if pct == 0:
                text = f'{format_number(total)}'
            else:
                if pct < 1:
                    text = f'{format_number(total)} (<1%)'
                else:
                    rounded = round(pct, 1)
                    if pct == rounded:
                        pct_str = f'{int(pct)}%'
                    else:
                        pct_str = f'{pct:.1f}%'
                    text = f'{format_number(total)} ({pct_str})'
            print(f'{row["label"]} - Total: {total}, Reverted: {row["reverted"]}, Non-reverted: {row["non_reverted"]}, Percentage: {pct:.1f}%')
        else:
            text = f'{format_number(total)}'
        ax.text(total * 1.05, i, text, va='center', ha='left', fontsize=11)

    ax.set_xscale('log')
    ax.set_yticks(y)
    ax.set_yticklabels(df['label'], fontsize=11)
    ax.set_xlabel('Number of changes (log scale)', fontsize=11)
    ax.tick_params(axis='x', labelsize=11)
    xmin, xmax = ax.get_xlim()
    ax.set_xlim(xmin, (xmax * 20))
    ax.set_ylim(-0.6, y[-1] + 0.6)
    ax.legend(loc='lower center', fontsize=11, frameon=False, bbox_to_anchor=(0.48, -0.25), handlelength=1.5, ncols=2)

    plt.tight_layout()
    os.makedirs(FIGURES_DIR, exist_ok=True)
    plt.savefig(out_path, dpi=300, bbox_inches='tight', pad_inches=0)
    plt.close(fig)


# -----------------------------------------------------------------------
# Plot 2: change type per datatype, rule-based vs. ml-based share
# -----------------------------------------------------------------------

def load_change_type_per_datatype():
    entity = load_csv('updates_entity_rev_stats')
    text = load_csv('updates_text_rev_stats')
    entity['total_counts'] = pd.to_numeric(entity['total_counts'])
    text['count_updates'] = pd.to_numeric(text['count_updates'])

    rows = []

    # entity: the rb column already tells us rule-based vs. ml-based
    for label, grp in entity.groupby('label'):
        rb_count = grp.loc[grp['rb'] == True, 'total_counts'].sum()
        ml_count = grp.loc[grp['rb'] == False, 'total_counts'].sum()
        rows.append(dict(datatype='entity', label=label, rb_count=rb_count, ml_count=ml_count))

    same_datatype_path = f'{DATA_DIR}/updates_same_datatype_stats.csv'

    same_datatype = read_csv_robust(same_datatype_path)
    same_datatype['count_updates'] = pd.to_numeric(same_datatype['count_updates'])
    same_datatype['new_datatype'] = same_datatype['new_datatype'].replace({
        'globecoordinate_latitude': 'globecoordinate',
        'globecoordinate_longitude': 'globecoordinate',
        'string': 'text',
    })
    # a row's label can be a comma-joined combo (e.g. a globecoordinate
    # update where latitude and longitude were each classified
    # separately) - split it out so each individual label gets counted,
    # matching how updates_text_rev_stats.sql unnests its label column.
    same_datatype = same_datatype.assign(label=same_datatype['label'].str.split(',')).explode('label')
    same_datatype['label'] = same_datatype['label'].str.strip()


    # text: ml-based counts come from updates_text_rev_stats (all ml-based),
    # rule-based counts come from updates_same_datatype_stats (new_datatype == 'string')
    text_ml = text.groupby('individual_label')['count_updates'].sum()
    if same_datatype is not None:
        text_rb = (
            same_datatype[same_datatype['new_datatype'] == 'text']
            .groupby('label')['count_updates'].sum()
        )
    else:
        text_rb = pd.Series(dtype=float)

    for label in sorted(set(text_ml.index) | set(text_rb.index)):
        rows.append(dict(
            datatype='text', label=label,
            rb_count=text_rb.get(label, 0),
            ml_count=text_ml.get(label, 0),
        ))

    # remaining datatypes (quantity, time, globe coordinate, ...): entirely
    # rule-based classified, no ml pass is run on them.
    if same_datatype is not None:
        other = same_datatype[same_datatype['new_datatype'] != 'text']
        for (datatype, label), grp in other.groupby(['new_datatype', 'label']):
            rows.append(dict(datatype=datatype, label=label, rb_count=grp['count_updates'].sum(), ml_count=0))

    df = pd.DataFrame(rows)
    df['total'] = df['rb_count'] + df['ml_count']
    df['rb_percentage'] = df['rb_count'] / df['total'] * 100
    df['display_label'] = df['label'].map(lambda l: DISPLAY_LABELS.get(l, l.replace('_', ' ')))

    print(df[['datatype', 'label', 'rb_count', 'ml_count', 'total', 'rb_percentage']])

    return df


def plot_change_type_per_datatype(df, out_path):
    datatypes = [dt for dt in ['entity', 'text', 'quantity', 'time', 'globecoordinate'] if dt in df['datatype'].unique()]
    datatypes += [dt for dt in df['datatype'].unique() if dt not in datatypes]

    n_labels = {dt: df[df['datatype'] == dt]['label'].nunique() for dt in datatypes}

    # one color per change type 
    palette = [c for name, c in TOL_MUTED.items() if name in ['olive', 'teal', 'rose', 'sand', 'indigo']]
    preferred_order = [l for l in DISPLAY_LABELS if l in df['label'].unique()]
    preferred_order += [l for l in df['label'].unique() if l not in preferred_order]
    label_color = {label: palette[i % len(palette)] for i, label in enumerate(preferred_order)}

    width_bars = 0.14
    x_start = {}
    cursor = 0
    for dt in datatypes:
        x_start[dt] = cursor / 5
        cursor += n_labels[dt]

    fig, ax = plt.subplots(figsize=(6, 5))
    x_positions = []
    all_bars = []

    for dt in datatypes:
        df_dt = df[df['datatype'] == dt].sort_values('label')
        for l_idx, (_, row) in enumerate(df_dt.iterrows()):
            x_pos = x_start[dt] + l_idx / 6
            x_positions.append(x_pos)
            color = label_color[row['label']]

            rb_bar = ax.barh(x_pos, row['rb_count'], color=color, edgecolor='black', linewidth=0.5, height=width_bars)
            ml_bar = ax.barh(x_pos, row['ml_count'], left=row['rb_count'], color=color, hatch='///', edgecolor='black', linewidth=0.5, height=width_bars)
            all_bars.append((rb_bar[0], ml_bar[0], row))

    for rb_bar, ml_bar, row in all_bars:
        bar_width = rb_bar.get_width() + ml_bar.get_width()
        bar_y = rb_bar.get_y() + rb_bar.get_height() / 2
        if int(row['rb_percentage']) == 100:
            label = format_number(row['total'])
        else:
            pct = row['rb_percentage']
            rounded = round(pct, 1)
            if int(pct) == rounded:
                pct_str = f'{int(pct)}%'
            else:
                pct_str = f'{pct:.1f}%'
            label = f"{format_number(row['total'])} ({pct_str} r-b)"
        ax.text(bar_width * 1.05, bar_y, label, ha='left', va='center', fontsize=10)

    ax.set_xscale('log')
    xmin, xmax = ax.get_xlim()
    ax.set_xlim(xmin, xmax * 30)
    ax.set_yticks(x_positions)
    ax.set_yticklabels([])

    group_centers = []
    for dt in datatypes:
        if n_labels[dt] % 2 == 0:
            group_centers.append(x_start[dt] + n_labels[dt] / 14)
        else:
            group_centers.append(x_start[dt] + (n_labels[dt] + 1) / 14)

    dt_display = {'globecoordinate': 'globe\ncoordinate'}
    ax_labels = ax.twinx()
    ax_labels.set_ylim(ax.get_ylim())
    ax_labels.set_yticks(group_centers)
    ax_labels.set_yticklabels([dt_display.get(dt, dt) for dt in datatypes], fontsize=11, multialignment='left')
    ax_labels.yaxis.set_ticks_position('left')
    ax_labels.yaxis.set_label_position('left')
    ax_labels.spines['left'].set_position(('outward', 5))
    ax_labels.set_frame_on(False)
    ax_labels.tick_params(left=False)

    for i, dt in enumerate(datatypes[1:], 1):
        prev_dt = datatypes[i - 1]
        last_bar_prev = x_start[prev_dt] + (n_labels[prev_dt] - 1) / 6
        first_bar_curr = x_start[dt]
        midpoint = (last_bar_prev + first_bar_curr) / 2
        ax.axhline(y=midpoint, color='black', linestyle='--', linewidth=1)

    ax.set_ylim(-0.2, cursor / 5 - 0.1)
    ax.set_xlabel('Number of changes (log scale)', fontsize=11)

    legend_elements = [
        Patch(facecolor=color, edgecolor='black', label=DISPLAY_LABELS.get(label, label.replace('_', ' ')))
        for label, color in label_color.items()
    ]
    legend_elements += [
        Patch(facecolor='white', edgecolor='black', label='rule-based (r-b)'),
        Patch(facecolor='white', edgecolor='black', hatch='///', label='ml-based'),
    ]
    fig.legend(handles=legend_elements, ncol=4, loc='outside lower center',frameon=False, fontsize=11, handlelength=1.5, bbox_to_anchor=(0.5, -0.08))

    plt.tight_layout()
    os.makedirs(FIGURES_DIR, exist_ok=True)
    plt.savefig(out_path, dpi=300, bbox_inches='tight', pad_inches=0)
    plt.close(fig)

# -----------------------------------------------------------------------
# Plot 3: revision and value change distribution over time 
# -----------------------------------------------------------------------
def plot_revision_value_change_distribution_over_time():
    df = pd.read_csv(f'{DATA_DIR}/entity_stats.csv')

    print('Number of unique entities:', df['entity_id'].nunique())

    max_revisions = df['num_revisions'].max()
    max_value_changes = df['num_value_changes'].max()
    print('Entity with highest number of revisions:', df[['entity_id', 'entity_label']][df['num_revisions'] == max_revisions], 'with', max_revisions, 'revisions')
    print('Entity with highest number of value changes:', df[['entity_id', 'entity_label']][df['num_value_changes'] == max_value_changes], 'with', max_value_changes, 'value changes')


    #  ------------ Distribution of revisions per entity ------------
    num_revisions = df['num_revisions']
    mean_val = num_revisions.mean()
    median_val = num_revisions.median()
    mode_val = num_revisions.mode().iloc[0]  # .mode() can return multiple values if tied; take the first

    print(f'Mean: {mean_val:.2f}')
    print(f'Median: {median_val:.2f}')
    print(f'Mode: {mode_val}')

    # log-spaced bins, since revision counts are typically heavily right-skewed
    min_val = max(num_revisions.min(), 1)  # avoid log(0)
    max_val = num_revisions.max()
    bins = np.logspace(np.log10(min_val), np.log10(max_val), 50)

    fig, ax = plt.subplots(figsize=(7, 5))
    ax.hist(num_revisions, bins=bins, edgecolor='black', linewidth=0.5, color='#88CCEE', alpha=0.7)

    ax.axvline(mean_val, color='#CC6677', linestyle='--', linewidth=1.5, label=f'Mean = {mean_val:.1f}')
    ax.axvline(median_val, color='#332288', linestyle='-.', linewidth=1.5, label=f'Median = {median_val:.1f}')
    ax.axvline(mode_val, color='#DDCC77', linestyle=':', linewidth=1.5, label=f'Mode = {mode_val}')

    ax.set_xscale('log')
    ax.set_yscale('log')  # entity counts per bucket are also likely to be skewed
    ax.set_xlabel('Number of revisions per entity (log scale)')
    ax.set_ylabel('Number of entities (log scale)')
    ax.legend()

    plt.tight_layout()
    plt.savefig(f'{FIGURES_DIR}/revisions_per_entity_histogram.png', dpi=300, bbox_inches='tight')
    plt.show()

    #  ------------- Distribution of value changes per entity -------------
    num_val_changes = df['num_value_changes']
    mean_val = num_val_changes.mean()
    median_val = num_val_changes.median()
    mode_val = num_val_changes.mode().iloc[0]  # .mode() can return multiple values if tied; take the first

    print(f'Mean: {mean_val:.2f}')
    print(f'Median: {median_val:.2f}')
    print(f'Mode: {mode_val}')

    # log-spaced bins, since revision counts are typically heavily right-skewed
    min_val = max(num_val_changes.min(), 1)  # avoid log(0)
    max_val = num_val_changes.max()
    bins = np.logspace(np.log10(min_val), np.log10(max_val), 50)

    fig, ax = plt.subplots(figsize=(7, 5))
    ax.hist(num_val_changes, bins=bins, edgecolor='black', linewidth=0.5, color='#88CCEE', alpha=0.7)

    ax.axvline(mean_val, color='#CC6677', linestyle='--', linewidth=1.5, label=f'Mean = {mean_val:.1f}')
    ax.axvline(median_val, color='#332288', linestyle='-.', linewidth=1.5, label=f'Median = {median_val:.1f}')
    ax.axvline(mode_val, color='#DDCC77', linestyle=':', linewidth=1.5, label=f'Mode = {mode_val}')

    ax.set_xscale('log')
    ax.set_yscale('log')  # entity counts per bucket are also likely to be skewed
    ax.set_xlabel('Number of value changes per entity (log scale)')
    ax.set_ylabel('Number of entities (log scale)')
    ax.legend()

    plt.tight_layout()
    plt.savefig(f'{FIGURES_DIR}/val_changes_per_entity_histogram.png', dpi=300, bbox_inches='tight')
    plt.show()

def main():
    dist_df = load_change_type_distribution()
    plot_change_type_distribution(dist_df, f'{FIGURES_DIR}/distribution_change_types_reverted.png')

    per_datatype_df = load_change_type_per_datatype()
    plot_change_type_per_datatype(per_datatype_df, f'{FIGURES_DIR}/distribution_change_types_per_datatype.png')


if __name__ == '__main__':
    main()
