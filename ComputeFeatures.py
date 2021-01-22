import json
from utils import *
import re
import functools
import csv
import logging
import operator


class DirectGraph:
    def __init__(self):
        self.graph_data = {}

    def update(self, edge_list):
        for edge in edge_list:
            if edge[0] in self.graph_data:
                if not edge[1] in self.graph_data[edge[0]]:
                    self.graph_data[edge[0]][edge[1]] = 0
            else:
                self.graph_data[edge[0]] = {edge[1]: 0}
            self.graph_data[edge[0]][edge[1]] += 1


class ReviewGraph(DirectGraph):
    def __init__(self):
        super().__init__()


class CommentsGraph(DirectGraph):
    def __init__(self):
        super().__init__()


class VotingGraph(DirectGraph):
    def __init__(self):
        super().__init__()

    def update(self, edge_list):
        for edge in edge_list:
            if edge[0] not in self.graph_data:
                self.graph_data[edge[0]] = {"-2": [], "-1": [], "0": [], "+1": [], "+2": []}

            self.graph_data[edge[0]][edge[1]].append(edge[2])


    def get(self, user_id):
        return VerifyAndGet(self.graph_data, user_id, {"-2": [], "-1": [], "0": [], "+1": [], "+2": []})


class PersonsChangesGraph(DirectGraph):
    def __init__(self):
        super().__init__()

    def update(self, edge_list):
        for edge in edge_list:
            if edge[0] not in self.graph_data:
                self.graph_data[edge[0]] = {'Own': [], 'Review': []}
            else:
                self.graph_data[edge[0]][edge[1]].append(edge[2])

    def get(self, user_id):
        return VerifyAndGet(self.graph_data, user_id, {'Own': [], 'Review': []})


"""                
class PersonFilesGraph(DirectGraph) : 
    def __init__(self) : 
        self.__init()__
        self.CurrentToOriginalNames = {}
    def update_graph(self,EdgeList) : 
         for edge in edgeList : 
            if len(edge[1]) == 2: 
                self.updateFilename(edge[1][0],edge[1][1])
                edge[1] = self.getOldestName(edge[1][0])
            if edge[0] not in self.graph_data : 
                self.graph_data[edge[0]] = []
            else :
                self.graph_data[edge[0]].append(edge[1])
    def UpdateFilename(self,old_name,new_name) : 
        self.CurrentToOriginalNames[new_name] = old_name 
    def getOldestName(self,name) : 
        current_name = name 
        current_old_name = VerifyAndGet(self.CurrentToOriginalNames,current_name,None)
        while current_old_name != None : 
            current_name = current_old_name
            current_old_name = VerifyAndGet(self.CurrentToOriginalNames,current_name,None) 
        return current_name
 """


class ComputeStatistics:
    def __init__(self, project_files_path, repos_path, bots_accounts_ids, metadata, results_path):

        self.graphs = {'experience_graph': PersonsChangesGraph(),
                       'review_graph': VotingGraph()
                       }
        self.repos_path = repos_path
        self.projects_files = project_files_path
        self.bots_account_ids = bots_accounts_ids
        self.metadata = metadata
        self.data = {}
        self.results_path = results_path

        os.makedirs(self.results_path, exist_ok=True)

    def process_review_data(self):

        current_status = None
        current_file_name = None
        file_data = None

        output_file = open(self.results_path + '/data.csv', mode='w', newline='')
        writer = None
        current_open_changes = {}
        for index, irow in self.metadata.iterrows():
            try:

                event_status = irow['event']

                if event_status == 'create':
                    if index % 1000 == 0:
                        print("current index : ", index)
                    file_name = irow["file_name"]
                    index = irow["index"]
                    if (current_file_name != file_name) or (current_status != irow['status']):
                        file_data = json.load(
                            open(os.path.join(self.projects_files, irow['status'], 'data', file_name)))
                        if len(file_data) == 2:
                            file_data = functools.reduce(operator.iconcat, file_data, [])
                        current_file_name = file_name
                        current_status = irow['status']

                    current_open_changes[irow['ID']] = ExtractChangeData(change=file_data[index],
                                                                         repos_path=self.repos_path,
                                                                         bots_names_list=self.bots_account_ids)
                    # compute feature with the current system state
                    row = {"id": current_open_changes[irow['ID']].change_id}
                    # patch features
                    change_metrics = current_open_changes[irow['ID']].compute_change_metrics()
                    row.update(change_metrics['product_metrics'])
                    author_id = current_open_changes[irow['ID']].authorAccountId['_account_id']
                    # computing author experience metrics
                    row.update(self.compute_experience(account_id=author_id, prefix='author'))
                    initial_reviewers_ids = [reviewer_data['_account_id'] for reviewer_data in
                                             current_open_changes[irow['ID']].reviewers_data.GetAllHumanReviewers()]
                    review_interaction_data = self.process_author_reviewers_history_data(initial_reviewers_ids,
                                                                                         author_id)
                    row.update(self.author_reviewers_interaction(review_interaction_data))
                    # write new row
                    if writer is None:
                        writer = csv.DictWriter(output_file, fieldnames=list(row.keys()))
                        writer.writeheader()
                    writer.writerow(row)
                else:
                    if irow['ID'] in current_open_changes:
                        human_reviewers_final_voting_infos = current_open_changes[
                            irow['ID']].reviewers_data.human_reviewer_with_code_review()
                        voting_edge_list = [
                            (infos['_account_id'], str(signed_representation(int(infos['value']))), current_open_changes[irow['ID']].change_id) for infos
                            in
                            human_reviewers_final_voting_infos]
                        person_change_graph_edge_list = [(user['_account_id'], "Review",
                                                          current_open_changes[irow['ID']].change_id) for user in
                                                         human_reviewers_final_voting_infos] + [
                                                            (
                                                                current_open_changes[irow['ID']].authorAccountId[
                                                                    '_account_id'],
                                                                "Own", current_open_changes[irow['ID']].change_id)]
                        # code review is over
                        # we update the data to
                        self.update_graphs(
                            {'experience_graph': person_change_graph_edge_list, 'review_graph': voting_edge_list})
                        # deleting the closed code change
                        del current_open_changes[irow['ID']]

            except Exception as e:
                logging.info("=============")
                logging.exception("message")
                # logging.info(current_open_changes[irow['ID']].id)

    def compute_experience(self, account_id, prefix=''):
        experience_for_given_account = self.graphs['experience_graph'].get(account_id)
        reviewing_exp = len(experience_for_given_account["Review"])
        authoring_exp = len(experience_for_given_account["Own"])
        return {prefix + '_authoring_experience': authoring_exp, prefix + '_reviewing_experience': reviewing_exp}

    def author_reviewers_interaction(self, review_interaction_data):
        output_data = {}
        reviewers_authoring_exp_statistics = compute_list_statistics('reviewers_authoring_exp', [
            review_interaction_data['reviewers_authoring_exp'][reviewer] for reviewer in
            review_interaction_data['reviewers_authoring_exp']])
        output_data.update(reviewers_authoring_exp_statistics)

        author_reviewers_coexp_statistics = compute_list_statistics('author_reviewers_coexp', [
            review_interaction_data['author_reviewers_co_review_exp'][reviewer] for reviewer in
            review_interaction_data['author_reviewers_co_review_exp']])
        output_data.update(author_reviewers_coexp_statistics)

        author_reviewers_positive_agreement_statistics = compute_list_statistics('author_reviewers_positive_agreement',
                                                                                 [review_interaction_data[
                                                                                      'author_reviewers_mutual_positive_agreement'][
                                                                                      reviewer] for reviewer in
                                                                                  review_interaction_data[
                                                                                      'author_reviewers_mutual_positive_agreement']])
        output_data.update(author_reviewers_positive_agreement_statistics)

        author_reviewers_mutual_negative_agreement_statistics = compute_list_statistics(
            'author_reviewers_mutual_negative_agreement',
            [review_interaction_data['author_reviewers_mutual_negative_agreement'][reviewer] for reviewer in
             review_interaction_data['author_reviewers_mutual_negative_agreement']])
        output_data.update(author_reviewers_mutual_negative_agreement_statistics)

        reviewers_positive_agreement_statistics = compute_list_statistics('reviewers_positive_agreement', [
            review_interaction_data['reviewers_positive_agreement'][reviewer] for reviewer in
            review_interaction_data['reviewers_positive_agreement']])
        output_data.update(reviewers_positive_agreement_statistics)

        reviewers_negative_agreement_statistics = compute_list_statistics('reviewers_negative_agreement', [
            review_interaction_data['reviewers_negative_agreement'][reviewer] for reviewer in
            review_interaction_data['reviewers_negative_agreement']])
        output_data.update(reviewers_negative_agreement_statistics)

        previous_positive_voting_history_statistics = compute_list_statistics('previous_positive_voting_history', [
            review_interaction_data['positive_previous_votes'][reviewer] for reviewer in
            review_interaction_data['positive_previous_votes']])
        output_data.update(previous_positive_voting_history_statistics)

        negative_previous_votes_statistics = compute_list_statistics('negative_previous_votes', [
            review_interaction_data['negative_previous_votes'][reviewer] for reviewer in
            review_interaction_data['negative_previous_votes']])
        output_data.update(negative_previous_votes_statistics)

        return output_data

    def process_author_reviewers_history_data(self, reviewers_ids, author_id):
        author_reviewers_co_review_exp = {}
        reviewers_authoring_exp = {}
        reviewers_review_exp = {}
        reviewers_co_review_exp = {}
        author_reviewers_mutual_positive_agreement = {}
        author_reviewers_mutual_negative_agreement = {}
        reviewers_positive_agreement = {}
        reviewers_negative_agreement = {}
        positive_previous_votes = {}
        negative_previous_votes = {}
        for counter1 in range(len(reviewers_ids)):
            first_person = reviewers_ids[counter1]
            if first_person == author_id:
                continue
            reviewers_authoring_exp[first_person] = self.compute_exp(first_person, "Own")
            reviewers_review_exp[first_person] = self.compute_exp(first_person, "Review")
            positive_previous_votes[first_person] = self.previous_voting_history(author_id, first_person, "+")
            negative_previous_votes[first_person] = self.previous_voting_history(author_id, first_person, "-")
            author_reviewers_co_review_exp[first_person] = self.mutual_co_review(first_person, author_id)
            author_reviewers_mutual_positive_agreement[first_person] = self.mutual_agreement(first_person,
                                                                                             author_id, "+")
            author_reviewers_mutual_negative_agreement[first_person] = self.mutual_agreement(first_person,
                                                                                             author_id, "-")
            if counter1 != len(reviewers_ids):
                for counter2 in range(counter1 + 1, len(reviewers_ids)):
                    second_person = reviewers_ids[counter2]
                    if second_person != author_id:
                        reviewers_co_review_exp[str(first_person) + "_" + str(second_person)] = self.mutual_co_review(
                            first_person, second_person)
                        reviewers_positive_agreement[
                            str(first_person) + "_" + str(second_person)] = self.mutual_agreement(first_person,
                                                                                                  second_person, "+")
                        reviewers_negative_agreement[
                            str(first_person) + "_" + str(second_person)] = self.mutual_agreement(first_person,
                                                                                                  second_person, "-")

        return {
            'author_reviewers_mutual_positive_agreement': author_reviewers_mutual_positive_agreement,
            'author_reviewers_mutual_negative_agreement': author_reviewers_mutual_negative_agreement,
            'author_reviewers_co_review_exp': author_reviewers_co_review_exp,
            'reviewers_authoring_exp': reviewers_authoring_exp,
            'reviewers_review_exp': reviewers_review_exp,
            'reviewers_co_review_exp': reviewers_co_review_exp,
            'reviewers_positive_agreement': reviewers_positive_agreement,
            'reviewers_negative_agreement': reviewers_negative_agreement,
            'positive_previous_votes': positive_previous_votes,
            'negative_previous_votes': negative_previous_votes}

    def mutual_agreement(self, reviewer1, reviewer2, sign):
        experience_graph = self.graphs['experience_graph']
        review_graph = self.graphs['review_graph']
        person1_reviews = set(experience_graph.get(reviewer1)["Review"])
        person2_reviews = set(experience_graph.get(reviewer2)["Review"])
        common_reviews = person1_reviews.intersection(person2_reviews)
        person1_signed_vote = set([changeid for score in ["-1", "-2", "+1", "+2"] if sign in score for changeid in
                                   review_graph.get(reviewer1)[score] if changeid in common_reviews])
        person2_signed_vote = set([changeid for score in ["-1", "-2", "+1", "+2"] if sign in score for changeid in
                                   review_graph.get(reviewer2)[score] if changeid in common_reviews])
        if len(person1_signed_vote.union(person2_signed_vote)) != 0:
            return len(person1_signed_vote.intersection(person2_signed_vote)) * 1.0 / len(
                person1_signed_vote.union(person2_signed_vote))
        return 0.0

    def previous_voting_history(self, person1, person2, sign):
        experience_graph = self.graphs['experience_graph']
        authored_changes = set(experience_graph.get(person1)["Own"])
        total_reviewed_changes = set([changeid for score in ["-1", "-2", "+1", "+2"] for changeid in
                                      self.graphs['review_graph'].get(person2)[score]])
        common_reviews = total_reviewed_changes.intersection(authored_changes)
        signed_reviewed_changes = set([changeid for score in ["-1", "-2", "+1", "+2"] if sign in score for changeid in
                                       self.graphs['review_graph'].get(person2)[score] if changeid in common_reviews])
        if len(common_reviews.union(signed_reviewed_changes)) != 0:
            return len(common_reviews.intersection(signed_reviewed_changes)) * 1.0 / (
                len(common_reviews.union(signed_reviewed_changes)))
        return 0.0

    def mutual_co_review(self, person1, person2):
        experience_graph = self.graphs['experience_graph']
        person1_reviews = set(experience_graph.get(person1)['Review'])
        person2_reviews = set(experience_graph.get(person2)['Review'])
        if (len(person1_reviews.union(person2_reviews)) != 0):
            return len(person1_reviews.intersection(person2_reviews)) * 1.0 / (
                len(person1_reviews.union(person2_reviews)))
        return 0.0

    def compute_exp(self, person, exp_type):
        experience_graph = self.graphs['experience_graph']
        return len(experience_graph.get(person)[exp_type])

    def update_graphs(self, updates):
        for graph_update in updates:
            self.graphs[graph_update].update(updates[graph_update])

    def compute_divergent_voting_patterns(self, revision_votes):
        divergent_labels_count = {"+2_-1": 0, "+2_-2": 0, "+1_-1": 0, "+1_-2": 0, "-2_+1": 0, "-2_+2": 0, "-1_+1": 0,
                                  "-1_+2": 0}
        for i in range(len(revision_votes) - 1):
            if revision_votes[i][0] != revision_votes[i + 1][0]:
                divergent_labels_count[str(revision_votes[i]) + "_" + str(revision_votes[i + 1])] += 1
        return divergent_labels_count


class ExtractChangeData:
    def __init__(self, change, repos_path, bots_names_list):
        self.change = change
        self.bots_names_list = bots_names_list
        self.change_id = VerifyAndGet(change, 'change_id', '')
        self.full_id = VerifyAndGet(change, 'id', '')
        self.branch = VerifyAndGet(change, "branch", '')
        self.repository_name = VerifyAndGet(change, "project", '')
        self.authorAccountId = VerifyAndGet(change, 'owner', {})

        self.messages_data = ExtractMessagesFeatures(VerifyAndGet(change, "messages", []),
                                                     list(VerifyAndGet(change, 'labels', {}).keys()))
        self.revisions_data = PatchsetData(repos_path, VerifyAndGet(change, "revisions", {}), self.repository_name)
        self.reviewers_data = ReviewersData(VerifyAndGet(change, "reviewers", []), self.bots_names_list, VerifyAndGet(
            VerifyAndGet(VerifyAndGet(change, "labels", {}), 'Code-Review', {}), 'all', {}), self.authorAccountId)
        self.compute_change_metrics()

    def compute_change_metrics(self):
        # self.currentRevisionsNumber = len(VerifyAndGet(self.change, 'revisions', []))
        # compute comments metrics
        # self.total_inline_comments = VerifyAndGet(self.change, 'total_comment_count', 0)
        # self.unsolved_comments = VerifyAndGet(self.change, 'unresolved_comment_count', 0)
        # compute first patchset files metrics metrics

        files_data = self.revisions_data.extract_revision_data_files_data(1)
        product_metrics = self.compute_product_metrics(files_data)

        # comments and other metrics are removed
        return {'product_metrics': product_metrics}

    def compute_product_metrics(self, files_data):
        product_metrics = {
            'added_lines': int(self.change['insertions']),
            'deleted_lines': int(self.change['deletions'])
        }
        product_metrics['total_chrun'] = product_metrics['added_lines'] - product_metrics['deleted_lines']
        files_change_data = VerifyAndGet(files_data, 'files_data_from_change', {})
        files_commit_data, is_valid = VerifyAndGet(files_data, 'files_data_from_commit', ([], False))
        product_metrics.update(ExtractChangeData.extract_files_metrics_from_change_data(files_change_data))
        product_metrics.update(ExtractChangeData.extract_files_metrics_from_commit_data(files_commit_data, is_valid))
        return product_metrics

    @staticmethod
    def extract_files_metrics_from_change_data(files_change_data):
        status_to_metric_name = {
            'M': 'modified_files_count',
            'A': 'added_files_count',
            'D': 'deleted_files_count',
            'R': 'renamed_files_count',
            'C': 'copied_files_count',
            'W': 'rewritten_files_count'
        }
        files_metrics_from_change_data = {
            'binary_files_count': 0,
            'change_files_count': len(list(files_change_data.keys()))
        }
        files_metrics_from_change_data.update({status_to_metric_name[status]: 0 for status in status_to_metric_name})

        for _, file_data in files_change_data.items():
            file_status = VerifyAndGet(file_data, 'status', 'M')
            files_metrics_from_change_data[status_to_metric_name[file_status]] += 1
            files_metrics_from_change_data['binary_files_count'] += VerifyAndGet(files_change_data, 'binary',
                                                                                 'false') == 'true'
        return files_metrics_from_change_data

    @staticmethod
    def extract_files_metrics_from_commit_data(all_files_commit_data, data_is_valid):
        if not data_is_valid:
            return {
                'total_complexity': None,
                'total_modified_methods_count': None,
                'total_LOC': None
            }
        files_metrics_from_commit_data = {
            'total_complexity': 0,
            'total_modified_methods_count': 0,
            'total_LOC': 0
        }
        for file_modification_data in all_files_commit_data:
            if file_modification_data["complexity"] is not None:
                files_metrics_from_commit_data['total_complexity'] += float(file_modification_data["complexity"])
            if file_modification_data["loc"] is not None:
                files_metrics_from_commit_data['total_LOC'] += int(file_modification_data["loc"])
            if file_modification_data['changed_methods_count'] is not None:
                files_metrics_from_commit_data['total_modified_methods_count'] += int(
                    file_modification_data['changed_methods_count'])
        return files_metrics_from_commit_data


class PatchsetData:
    def __init__(self, repos_path, patchsets_data, project_name):
        self.patchsets_data = patchsets_data
        self.repos_path = repos_path
        self.project_name = project_name

    def extract_revision_data_files_data(self, revision_number):
        for commit_hash in self.patchsets_data:
            if int(self.patchsets_data[commit_hash]['_number']) == revision_number:
                return {
                    'files_data_from_commit': self.extract_revision_files_modifications_with_commit_hash(commit_hash),
                    'files_data_from_change': self.extract_revision_files_data_from_change(commit_hash)
                }
        return {}

    def extract_revision_files_modifications_with_commit_hash(self, commit_hash):
        try:
            return extract_commit_data(self.repos_path, self.project_name, commit_hash), True
        except:
            return [], False

    def extract_revision_files_data_from_change(self, commit_hash):
        return VerifyAndGet(VerifyAndGet(self.patchsets_data, commit_hash, {}), 'files', {})


class ReviewersData:
    def __init__(self, reviewrs_data, bot_account_ids, code_review_data, author_data):
        self.reviewers_data = reviewrs_data
        self.bots_accounts_ids = bot_account_ids
        self.code_review_data = code_review_data
        self.author_data = author_data

    def GetAllHumanReviewers(self):
        try:
            return [account for account in self.all_reviewers_list() if
                    not account['_account_id'] in self.bots_accounts_ids and account['_account_id'] != self.author_data[
                        '_account_id']]
        except:
            return []

    def all_reviewers_list(self):
        return VerifyAndGet(self.reviewers_data, 'REVIEWER', []) + VerifyAndGet(self.reviewers_data, 'CC',
                                                                                []) + VerifyAndGet(self.reviewers_data,
                                                                                                   'REMOVED', [])

    def human_reviewer_with_code_review(self):
        return [account for account in self.code_review_data if
                not account['_account_id'] in self.bots_accounts_ids and int(account['value']) != 0 and account[
                    '_account_id'] !=
                self.author_data['_account_id']]


class ExtractMessagesFeatures:
    def __init__(self, messages_data, review_labels):
        self.messages = messages_data
        self.review_labels = review_labels

    def process_comments(self):
        """
        extracting messages data
        """
        messages_data = []
        for message in self.messages:
            message_revision_number = VerifyAndGet(message, "_revision_number", 1)
            message_date = message["date"]
            message_id = message["id"]
            original_text = message["message"]
            message_text = message["message"]
            message_real_author_information = VerifyAndGet(message, 'real_author', VerifyAndGet(message, 'author', ""))
            message_text = remove_line_breaks_redundant_spaces_tabs(message_text)
            message_text = self.removing_patchset_number(message_text, message_revision_number)
            message_text = remove_line_breaks_redundant_spaces_tabs(message_text)
            message_text, labels = self.remove_labels_prefixes(message_text)
            message_text, inline_comments_number = self.ExtractInlineCommentsNumberFromMessage(message_text)

            message_data = {
                "original_text": original_text,
                "message_id": message_id,
                "revision_number": message_revision_number,
                "message_date": message_date,
                "message_labels": labels,
                "inline_comments_number": inline_comments_number,
                "real_author": message_real_author_information
            }
            messages_data.append(message_data)
        return messages_data

    def remove_labels_prefixes(self, message_text):
        message = message_text
        removed_labels = {}
        label_exist = True
        while label_exist:
            label_exist = False
            for label_name in self.review_labels:
                if message.startswith(label_name):
                    message = remove_prefix(message, label_name)
                    label_exist = True
                    possible_labels_values = ["-2", "-1", "+1", "+2"]
                    for value in possible_labels_values:
                        if message.startswith(value):
                            removed_labels[label_name] = value
                            message = remove_prefix(message, value)
                    message = remove_prefix(message, " ")
        return message_text, removed_labels

    @staticmethod
    def removing_patchset_number(message_text, revision_number):
        message = remove_prefix(message_text, "Uploaded patch set " + str(revision_number) + ".")
        message = remove_prefix(message, "Patch Set " + str(revision_number) + ":")
        return message

    def ExtractInlineCommentsNumberFromMessage(self, message_text):
        message = message_text
        comments_number = re.findall(r'^\(\d+ comment[s]*\)', message_text)
        inline_comments_number = 0
        if len(comments_number) > 0:
            inline_comments_number = int(comments_number[0].split(" ")[0][1:])
            message = remove_prefix(message, comments_number[0])
        return message, inline_comments_number
