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

# How to contribute to Apache CarbonData

The Apache CarbonData community welcomes all kinds of contributions from anyone with a passion for
faster data format! Apache CarbonData is a new file format for faster interactive query using
advanced columnar storage, index, compression and encoding techniques to improve computing
efficiency,in turn it will help speedup queries an order of magnitude faster over PetaBytes of data.

We use a review-then-commit workflow in CarbonData for all contributions.

* Engage -> Design -> Code -> Review -> Commit

## Engage

### Mailing list(s)

We discuss design and implementation issues on dev@carbondata.incubator.apache.org Join by
emailing dev-subscribe@carbondata.incubator.apache.org

### Apache JIRA

We use [Apache JIRA](https://issues.apache.org/jira/browse/CARBONDATA) as an issue tracking and
project management tool, as well as a way to communicate among a very diverse and distributed set
of contributors. To be able to gather feedback, avoid frustration, and avoid duplicated efforts all
CarbonData-related work should be tracked there.

If you do not already have an Apache JIRA account, sign up [here](https://issues.apache.org/jira/).

If a quick search doesn’t turn up an existing JIRA issue for the work you want to contribute,
create it. Please discuss your proposal with a committer or the component lead in JIRA or,
alternatively, on the developer mailing list(dev@carbondata.incubator.apache.org).

If there’s an existing JIRA issue for your intended contribution, please comment about your
intended work. Once the work is understood, a committer will assign the issue to you.
(If you don’t have a JIRA role yet, you’ll be added to the “contributor” role.) If an issue is
currently assigned, please check with the current assignee before reassigning.

For moderate or large contributions, you should not start coding or writing a design doc unless
there is a corresponding JIRA issue assigned to you for that work. Simple changes,
like fixing typos, do not require an associated issue.

### Design

To avoid potential frustration during the code review cycle, we encourage you to clearly scope and
design non-trivial contributions with the CarbonData community before you start coding.

Generally, the JIRA issue is the best place to gather relevant design docs, comments, or references.
It’s great to explicitly include relevant stakeholders early in the conversation. For designs that
may be generally interesting, we also encourage conversations on the developer’s mailing list.

### Code

We use GitHub’s pull request functionality to review proposed code changes.
If you do not already have a personal GitHub account, sign up [here](https://github.com).

### Git config

Ensure to finish the below config(user.email, user.name) before starting PR works.
```
$ git config --global user.email "you@example.com"
$ git config --global user.name "Your Name"
```

#### Fork the repository on GitHub

Go to the [Apache CarbonData GitHub mirror](https://github.com/apache/incubator-carbondata) and
fork the repository to your own private account.
This will be your private workspace for staging changes.

#### Clone the repository locally

You are now ready to create the development environment on your local machine.
Clone CarbonData’s read-only GitHub mirror.
```
$ git clone https://github.com/apache/incubator-carbondata.git
$ cd incubator-carbondata
```
Add your forked repository as an additional Git remote, where you’ll push your changes.
```
$ git remote add <GitHub_user> https://github.com/<GitHub_user>/incubator-carbondata.git
```
You are now ready to start developing!

#### Create a branch in your fork

You’ll work on your contribution in a branch in your own (forked) repository. Create a local branch,
initialized with the state of the branch you expect your changes to be merged into.
Keep in mind that we use several branches, including master, feature-specific, and
release-specific branches. If you are unsure, initialize with the state of the master branch.
```
$ git fetch --all
$ git checkout -b <my-branch> origin/master
```
At this point, you can start making and committing changes to this branch in a standard way.

#### Syncing and pushing your branch

Periodically while you work, and certainly before submitting a pull request, you should update
your branch with the most recent changes to the target branch.
```
$ git pull --rebase
```
Remember to always use --rebase parameter to avoid extraneous merge commits.

To push your local, committed changes to your (forked) repository on GitHub, run:
```
$ git push <GitHub_user> <my-branch>
```
#### Testing

All code should have appropriate unit testing coverage. New code should have new tests in the
same contribution. Bug fixes should include a regression test to prevent the issue from reoccurring.

For contributions to the Java code, run unit tests locally via Maven.
```
$ mvn clean verify
```

### Review

Once the initial code is complete and the tests pass, it’s time to start the code review process.
We review and discuss all code, no matter who authors it. It’s a great way to build community,
since you can learn from other developers, and they become familiar with your contribution.
It also builds a strong project by encouraging a high quality bar and keeping code consistent
throughout the project.

#### Create a pull request

Organize your commits to make your reviewer’s job easier. Use the following command to
re-order, squash, edit, or change description of individual commits.
```
$ git rebase -i origin/master
```
Navigate to the CarbonData GitHub mirror to create a pull request. The title of the pull request
should be strictly in the following format:
```
[CARBONDATA-issue number>] Title of the pull request
```
Please include a descriptive pull request message to help make the reviewer’s job easier.

If you know a good committer to review your pull request, please make a comment like the following.
If not, don’t worry, a committer will pick it up.
```
Hi @<committer/reviewer name>, can you please take a look?
```

#### Code Review and Revision

During the code review process, don’t rebase your branch or otherwise modify published commits,
since this can remove existing comment history and be confusing to the reviewer,
When you make a revision, always push it in a new commit.

Our GitHub mirror automatically provides pre-commit testing coverage using Jenkins.
Please make sure those tests pass,the contribution cannot be merged otherwise.

#### LGTM
Once the reviewer is happy with the change, they’ll respond with an LGTM (“looks good to me!”).
At this point, the committer will take over, possibly make some additional touch ups,
and merge your changes into the codebase.

In the case both the author and the reviewer are committers, either can merge the pull request.
Just be sure to communicate clearly whose responsibility it is in this particular case.

Thank you for your contribution to Apache CarbonData!

#### Deleting your branch(optional)
Once the pull request is merged into the Apache CarbonData repository, you can safely delete the
branch locally and purge it from your forked repository.

From another local branch, run:
```
$ git fetch --all
$ git branch -d <my-branch>
$ git push <GitHub_user> --delete <my-branch>
```
