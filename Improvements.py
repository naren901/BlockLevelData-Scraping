import bs4
from github import Github
import json
import os
import datetime


def get_commit_details_dictionary(commit_raw_data):

    author_name = ""
    author_email = ""
    author_date = ""
    commitor_name = ""
    commitor_email = ""
    commit_date = ""
    commit_verified = ""
    commit_reason = ""

    commit_sha = commit_raw_data.get("sha", "")
    author = commit_raw_data.get("author", None)
    if author != None:
        author_name = author.get("name", "")
        author_email = author.get("email", "")
        author_date = author.get("date", "").__str__()

    commiter = commit_raw_data.get("committer", None)
    if commiter != None:
        commitor_name = author.get("name", "")
        commitor_email = author.get("email", "")
        commit_date = author.get("date", "").__str__()

    commit_message = commit_raw_data.get("message", "")
    commit_verification = commit_raw_data.get("verification", None)
    if commit_verification != None:
        commit_verified = commit_verification.get("verified", "").__str__()
        commit_reason = commit_verification.get("reason", "")

    commit_dict = { "author_name": author_name, \
            "author_email": author_email, \
            "author_date": author_date, \
            "commitor_name": commitor_name, \
            "commitor_email": commitor_email, \
            "commit_date": commit_date, \
            "commit_message": commit_message, \
            "commit_verified": commit_verified, \
            "commit_reason": commit_reason  }

    return (commit_dict)


### Returns a dictionary
def get_content_details(current_content, repo_data):
    content_data_list = []
    raw_data = current_content.raw_data
    name = raw_data["name"]
    type = raw_data["type"]
    sha = raw_data["sha"]

    if type == "file":
        file_commits_data = []
        path = raw_data["path"]
        html_link = raw_data["html_url"]

        file_commits = repo_data.get_commits(path = path)
        commit_total =  file_commits.totalCount
        current_commit_count = 1
        for commit in file_commits:
            commit_data_dic = get_commit_details_dictionary (commit.commit.raw_data)
            file_commits_data.append(commit_data_dic)
            current_commit_count += 1

        content_dict_obj = {"name": name, \
                            "path": path, \
                            "sha": sha, \
                            "type": type,\
                            "html_link" : html_link,\
                            "Commits": file_commits_data}

        return (content_dict_obj)

    elif type == "dir":
        path = raw_data["path"]
        inner_contents = repo_data.get_contents(path)
        for inner_content in inner_contents:
            inner_content_details = get_content_details(inner_content,  repo_data)
            content_data_list.extend(inner_content_details)

    return (content_data_list)



def get_improvement_repo_details (gh, address):

    cwd = os.getcwd()
    for address in address.items():
        main_dir_path = os.path.join(cwd,"Improvements", address[0])

    if not os.path.exists(main_dir_path):
        os.makedirs(main_dir_path )

    try:
        repo_json_dict = {}
        repo_data = gh.get_repo(address[1])
        raw_data = repo_data.raw_data
        repo_name = raw_data.get("name", "").__str__()
        repo_json_dict["repo_id"] = raw_data.get("id", "").__str__()
        repo_json_dict["repo_name"] = repo_name
        repo_json_dict["repo_fullname"] = raw_data.get("full_name", "").__str__()
        repo_json_dict["description"] = raw_data.get("description", "").__str__()
        repo_json_dict["repo_forked"] = raw_data.get("fork", "").__str__()
        repo_json_dict["repo_language"] = raw_data.get("language", "")
        repo_json_dict["repo_createdat"] = raw_data.get("created_at", "").__str__()
        repo_json_dict["repo_updatedat"] = raw_data.get("updated_at", "").__str__()
        repo_json_dict["repo_watchers"] = raw_data.get("watchers", 0).__str__()
        repo_json_dict["repo_stargazers"] = raw_data.get("stargazers_count", 0).__str__()
        repo_json_dict["repo_wiki"] = raw_data.get("has_wiki", 0).__str__()
        repo_json_dict["repo_license_dict"] = raw_data.get("license", "")

        repo_file_path = os.path.join(main_dir_path,  repo_name + "_" + repo_json_dict["repo_id"]  + "." + "json")
        with open(repo_file_path, "w") as write_file:
            json.dump(repo_json_dict, write_file, indent=2)

        #### IMPROVEMENTS
        contents_dic = {}
        contents = repo_data.get_contents("")
        contents_total = len(contents)
        current_content = 1
        improvement_contents = []
        commit_file_path =os.path.join(main_dir_path,  repo_name + "_" + repo_json_dict["repo_id"] +"Improvements" + "." + "json")
        for content in contents:
            try:
                improvmentcontent = get_content_details(content,  repo_data)
                improvement_contents.append (improvmentcontent)

                commit_file_path = os.path.join(main_dir_path, repo_name + "_" + repo_json_dict[
                    "repo_id"] + "Improvements_"+current_content.__str__() + "." + "json")

                with open(commit_file_path, "a") as write_file:
                    json.dump(improvement_contents, write_file, indent=2)

                current_content += 1

            except Exception as  ContentEx:
                with open("improvements.log", mode="a") as logfile:
                    logfile.write(f" \n ERROR CONTENT LEVEL: {datetime.datetime.now().__str__()} {repo_name}, content number\
                                                                        {current_content.__str__()}\n {ContentEx.__str__()}\n")


        #repo_json_dict["content"] = contents_dic
        #with open(repo_bips.name + "." + "json", "w") as write_file:
            #json.dump(repo_json_dict, write_file)

    except Exception as top_exception:
        with open("improvements.log", mode="a") as logfile:
            logfile.write(" \n ERROR REPO LEVEL: {0} {1} \n {2}\n".format(datetime.datetime.now().__str__(),\
                                                                      repo_name,\
                                                                      top_exception.__str__()))

def main():
    token_narendra = "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"
    token_santhosh = "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"
    token_narendra003 = "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"

    gh = Github(token_narendra003)
    improvement_proposals_github = { \
        "Bitcoin": "bitcoin-sv-specs/bips", \
        "Bitcoin-Cash": "mpatc/bcips", \
        "Bitcoin-SV": "bitcoin-sv-specs/bips", \
        "Dash": "dashpay/dips", \
        "Dogecoin": "dogecoin/dips", \
        "Litecoin": "litecoin-project/lips", \
        #"Ethereum": "https://eips.ethereum.org"\
        }


    github_address = {"Bitcoin-SV": "bitcoin-sv-specs/bips"}
    get_improvement_repo_details(gh, github_address)

main()

