import re
import numpy as np
from pydriller import RepositoryMining
import os
def signed_representation(value) :
    if value > 0 :
        return ('+'+str(value))
    return str(value)
def VerifyAndGet(dict_data, key, default=None):
    if key in dict_data.keys():
        return dict_data[key]
    return default


def remove_line_breaks_redundant_spaces_tabs(message):
    return re.sub('\s+', ' ', message.replace('\r', '')).strip()


def remove_prefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix):]
    return text


def compute_list_statistics(feature_name, values, functions={'min': lambda statistics: min(statistics),
                                                             'max': lambda statistics: max(statistics),
                                                             'mean': lambda statistics: np.mean(statistics),
                                                             'std': lambda statistics: np.std(statistics)}):
    if len(values) == 0:
        return {function + '_' + feature_name: 0.0 for function in functions}
    return {function + '_' + feature_name: functions[function](values) for function in functions}


def extract_commit_data(repos_path, repo_name, commit_hash):
    # making results path
    commits = RepositoryMining(os.path.join(repos_path, repo_name), single=commit_hash).traverse_commits()
    all_data = []
    for commit in commits:
        for modification in commit.modifications:
            modification_data = {
                'changed_methods_count': len(modification.changed_methods),
                'added_lines': modification.added,
                "removed_lines": modification.removed,
                "loc": modification.nloc,
                "complexity": modification.complexity,
                "change_type": modification.change_type,
                "old_path": modification.old_path,
                'new_path': modification.new_path,
                'file_name': modification.filename
            }

            if modification_data["old_path"] is None:
                modification_data["old_path"] = ''
            if modification_data["new_path"] is None:
                modification_data["new_path"] = ''
                modification_data['file_path'] = modification_data["old_path"]
            all_data.append(modification_data)
    return all_data
