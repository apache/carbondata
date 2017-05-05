<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->
# How to merge pull requests using the carbon_pr.py

This code is heavily inspired by the similar tool in Apache Falcon.

This section documents the process for reviewing and merging code changes contributed via Github pull requests. It assumes you have a clone of carbondata's Git repository.
carbon_pr.py is a script that automates the process of accepting a code change into the project. It creates a temporary branch from apache/master, squashes the commits in the pull request, rewrites the commit message in the squashed commit to follow a standard format including information about each original commit, merges the squashed commit into the temporary branch, pushes the code to apache/trunk and closes the JIRA ticket. The push will then be mirrored to apache-github/master, which will cause the PR to be closed due to the pattern in the commit message. Note that the script will ask the user before executing remote updates (i.e. git push and closing JIRA ticket), so it can still be used even if the user wants to skip those steps.

## Installing Dependencies (Optional)
If you want to close JIRA issues using the script then you will need to install `jira` library.
You can install it using `easy_install` with 

`sudo easy_install jira`

or using pip with

`pip install jira`


## Setting up
The script requires two remotes to be configured - one for the github repo of the project(to download pull requests),
and one for the git repo of the project(to push the changes). 
  
1. Add aliases for the remotes expected by the merge script:
 
git remote add apache https://git-wip-us.apache.org/repos/asf/carbondata.git
 
git remote add apache-github https://github.com/apache/carbondata.git
 
If you execute the script without doing any setup,
the script will output the commands that you need to execute to setup these remotes. 



## Executing the tool
Once the pull request is ready to be merged (it has been reviewed, feedback has been addressed, CI build has been successful and the branch merges cleanly into trunk):

1. Run the merge script

 `python carbon_pr.py`
 
2. Answer the questions prompted by the script.



**TIP**: If you don't want to enter your jira credentials again and again then you can 
set the `JIRA_USERNAME` and `JIRA_PASSWORD` environment variables and the script will
use them instead of prompting for credentials.