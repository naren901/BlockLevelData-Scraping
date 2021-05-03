import bs4
from github import Github
import json
import os
import datetime

def get_each_repo_metadata (gh, addresses):
    cwd = os.getcwd()
    for address in addresses.items():
        main_dir_path = os.path.join(cwd, address[0])

        if not os.path.exists(main_dir_path):
            os.mkdir(main_dir_path)

        orgniz = gh.get_organization(address[1])
        raw_data = orgniz.raw_data
        repos= orgniz.get_repos()

        for repo in repos:
            try:
            ### collect repo information
                repo_data_dict = {}
                raw_data =  repo.raw_data
                repo_name = raw_data.get("name", "")
                if repo_name.lower() in ["bitcoin-tool",\
                                         "libdohj",\
                                         "dash",\
                                         "dash-binaries",\
                                         "dash-stratum",\
                                         "dash-roadmap",\
                                         "docker-dashd",\
                                         "dash-detached",\
                                         "dash-detached-sigs",\
                                         "dash_hash",\
                                         "dash-website",\
                                         "darkcoin-sgminer",\
                                         "docker-sentinel",\
                                         "electrum-dash-server",\
                                         "electrum-dash-old", \
                                         "godash",\
                                         "godashutil",\
                                         "godashutil",\
                                         "gitian-builder",\
                                         "gitian.sigs",\
                                         "keyhunter",\
                                         "key_finder",\
                                         "keepkey-firmware",\
                                         "p2pool-dash",\
                                         "node-x11-hash",\
                                         "node-stratum-pool",\
                                         "node-open-mining-portal",\
                                         "paper.dash.org",\
                                         "sentinel",\
                                         "trezor-mcu",\
                                         "x11-hash-js",\
                                         "electrum-dash",\
                                          "dips",\
                                          "dash-website-api",\
                                          "docs",\
                                          "bls-signatures",\
                                          "dash-abe"]:
                    continue

                repo_dir_path = os.path.join(main_dir_path,repo_name )
                if not os.path.exists(repo_dir_path):
                    os.mkdir(repo_dir_path)

                repo_fullname = raw_data.get("full_name", "")
                repo_id = raw_data.get("id", "").__str__()
                repo_desc = raw_data.get("description", "")
                repo_forked = raw_data.get("fork", "").__str__()
                repo_language = raw_data.get("language",  "")
                repo_createdat = raw_data.get("created_at", "") .__str__()
                repo_watchers = raw_data.get("watchers" , 0).__str__()
                repo_forks = raw_data.get("forks",  0).__str__()
                repo_nw_count = raw_data.get("network_count",  0).__str__()
                repo_sub_count = raw_data.get("subscribers_count",  0).__str__()
                repo_license_dict = raw_data.get("license",  "")

                repo_data_dict = {'name': repo_name, \
                                  "id": repo_id, \
                                  "fullname": repo_fullname, \
                                  "desc": repo_desc, \
                                  "forked": repo_forked, \
                                  "language": repo_language, \
                                  "createdat": repo_createdat, \
                                  "watchers": repo_watchers, \
                                  "forks": repo_forks, \
                                  "nw_count": repo_nw_count, \
                                  "sub_count": repo_sub_count, \
                                  "license_dict": repo_license_dict}

                repo_file_path = os.path.join(repo_dir_path,address[0]+"_" + repo_name + "_" + repo_id +  "." + "json" )
                with open(repo_file_path, "w") as write_file:
                    json.dump(repo_data_dict, write_file,indent= 2)

                commit_counter = 1
                commit_start_at = 1
                if repo_name.lower() == "relic":
                    commit_start_at = 1062 + 1

                commit_file_path = os.path.join(repo_dir_path, repo_name + "_" + repo_id + "_commit_"+"New_" + commit_start_at.__str__() + "." + "json")
                #commit_write_file = open(commit_file_path, "a")

                try:
                    commits = repo.get_commits()
                    commints_count = commits.totalCount

                    for commit in commits:
                        commit_dict = {}
                        author_name = ""
                        author_email = ""
                        author_date = ""
                        commitor_name = ""
                        commitor_email = ""
                        commit_date = ""
                        commit_verified = ""
                        commit_reason = ""

                        if commit_counter >= commit_start_at:
                            commit_rawdata = commit.commit.raw_data
                            commit_sha = commit_rawdata.get("sha", "")

                            author = commit_rawdata.get("author",None)
                            if author != None:
                                author_name = author.get("name", "")
                                author_email = author.get("email", "")
                                author_date = author.get("date", "").__str__()

                            commiter = commit_rawdata.get("committer", None)
                            if commiter != None:
                                commitor_name = author.get("name", "")
                                commitor_email = author.get("email", "")
                                commit_date = author.get("date", "").__str__()

                            commit_message = commit_rawdata.get("message", "")

                            commit_verification = commit_rawdata.get("verification", None)
                            if commit_verification != None:
                                commit_verified = commit_verification.get("verified", "").__str__()
                                commit_reason = commit_verification.get("reason", "")

                            commit_dict[commit_sha] = {
                                "author_name" :author_name,\
                                "author_email": author_email,\
                                "author_date" :author_date,\
                                "commitor_name" :commitor_name,\
                                "commitor_email" :commitor_email,\
                                "commit_date":commit_date,\
                                "commit_message":commit_message,\
                                "commit_verified": commit_verified,\
                                "commit_reason":commit_reason
                            }

                            #commit_file_path = os.path.join(repo_dir_path, repo_name + "_" + repo_id + "_commit_" +commit_counter + "." + "json")
                            ### write commit details to json file
                            with open(commit_file_path, "a") as write_file:
                                json.dump(commit_dict, write_file, indent=2)

                        commit_counter += 1
                        if commints_count == commit_counter:
                            completedhandle = open("completed_repos.txt",mode = "a")
                            completedhandle.write("Completed repo : " + repo_name + " Iterated commits are : "+ commit_counter.__str__()  + ", Total Commits are : " + commints_count.__str__() + "\n")

                except Exception as commitex:
                    with open("error.log", mode="a") as logfile:
                        logfile.write("\n ERROR COMMIT WRITING: \n {0}--{1}--Commit Number {2}\n {3} \n".format(datetime.datetime.now().__str__(), repo_name, commit_counter, commitex.__str__()))


            except Exception as ex:
                with open("error.log", mode="a") as logfile:
                    logfile.write(" \n ERROR REPO LEVEL: {0} {1} \n {2}\n".format(datetime.datetime.now().__str__(), repo_name, ex.__str__()))


def main():
    token_narendra = "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"
    token_santhosh = "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"
    token_narendra003 = "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"

    gh = Github(token_santhosh)
    '''
    github_addresses = { \
        "Bitcoin": "bitcoin", \
        "Bitcoin-Cash": "mpatc", \
        "Bitcoin-SV": "bitcoin-sv-specs", \
        "Dash": "dashpay", \
        "Dogecoin": "dogecoin", \
        "Litecoin": "litecoin-project", \
        #"Ethereum": "https://eips.ethereum.org"\
        }
    '''

    github_addresses = {"dash": "dashpay"}
    get_each_repo_metadata(gh, github_addresses)

main()