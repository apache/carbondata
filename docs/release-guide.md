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

# Apache CarbonData Release Guide

Apache CarbonData periodically declares and publishes releases.

Each release is executed by a _Release Manager_, who is selected among the CarbonData committers.
 This document describes the process that the Release Manager follows to perform a release. Any 
 changes to this process should be discussed and adopted on the 
 [dev@ mailing list](mailto:dev@carbondata.incubator.apache.org).
 
Please remember that publishing software has legal consequences. This guide complements the 
foundation-wide [Product Release Policy](http://www.apache.org/dev/release.html) and [Release 
Distribution Policy](http://www.apache.org/dev/release-distribution).

## Decide to release

Deciding to release and selecting a Release Manager is the first step of the release process. 
This is a consensus-based decision of the entire community.

Anybody can propose a release on the dev@ mailing list, giving a solid argument and nominating a 
committer as the Release Manager (including themselves). There's no formal process, no vote 
requirements, and no timing requirements. Any objections should be resolved by consensus before 
starting the release.

_Checklist to proceed to next step:_

1. Community agrees to release
2. Community selects a Release Manager

## Prepare for the release

Before your first release, you should perform one-time configuration steps. This will set up your
 security keys for signing the artifacts and access release repository.
 
To prepare for each release, you should audit the project status in the Jira, and do necessary 
bookkeeping. Finally, you should tag a release.

### One-time setup instructions

#### GPG Key

You need to have a GPG key to sign the release artifacts. Please be aware of the ASF-wide 
[release signing guidelines](https://www.apache.org/dev/release-signing.html). If you don't have 
a GPG key associated with your Apache account, please create one according to the guidelines.

Determine your Apache GPG key and key ID, as follows:

```
gpg --list-keys
```

This will list your GPG keys. One of these should reflect your Apache account, for exemple:

```
pub   2048R/845E6689 2016-02-23
uid                  Nomen Nescio <anonymous@apache.org>
sub   2048R/BA4D50BE 2016-02-23
```

Here, the key ID is the 8-digit hex string in the `pub` line: `845E6689`.

Now, add your Apache GPG key to the CarbonData's `KEYS` file in `dev` and `release` repositories 
at `dist.apache.org`. Follow the instructions listed at the top of these files.
 
Configure `git` to use this key when signing code by giving it your key ID, as follows:

```
git config --global user.signingkey 845E6689
```

You may drop the `--global` option if you'd prefer to use this key for the current repository only.

You may wish to start `gpg-agent` to unlock your GPG key only once using your passphrase. 
Otherwise, you may need to enter this passphrase several times. The setup of `gpg-agent` varies 
based on operating system, but may be something like this:

```
eval $(gpg-agent --daemon --no-grab --write-env-file $HOME/.gpg-agent-info)
export GPG_TTY=$(tty)
export GPG_AGENT_INFO
```

#### Access to Apache Nexus

Configure access to the [Apache Nexus repository](https://repository.apache.org), used for 
staging repository and promote the artifacts to Maven Central.

1. You log in with your Apache account.
2. Confirm you have appropriate access by finding `org.apache.carbondata` under `Staging Profiles`.
3. Navigate to your `Profile` (top right dropdown menu of the page).
4. Choose `User Token` from the dropdown, then click `Access User Token`. Copy a snippet of the 
Maven XML configuration block.
5. Insert this snippet twice into your global Maven `settings.xml` file, typically `${HOME]/
.m2/settings.xml`. The end result should look like this, where `TOKEN_NAME` and `TOKEN_PASSWORD` 
are your secret tokens:

```
 <settings>
   <servers>
     <server>
       <id>apache.releases.https</id>
       <username>TOKEN_NAME</username>
       <password>TOKEN_PASSWORD</password>
     </server>
     <server>
       <id>apache.snapshots.https</id>
       <username>TOKEN_NAME</username>
       <password>TOKEN_PASSWORD</password>
     </server>
   </servers>
 </settings>
```

#### Create a new version in Jira

When contributors resolve an issue in Jira, they are tagging it with a release that will contain 
their changes. With the release currently underway, new issues should be resolved against a 
subsequent future release. Therefore, you should create a release item for this subsequent 
release, as follows:

1. In Jira, navigate to `CarbonData > Administration > Versions`.
2. Add a new release: choose the next minor version number compared to the one currently 
underway, select today's date as the `Start Date`, and choose `Add`. 

#### Triage release-blocking issues in Jira

There could be outstanding release-blocking issues, which should be triaged before proceeding to 
build the release. We track them by assigning a specific `Fix Version` field even before the 
issue is resolved.

The list of release-blocking issues is available at the [version status page](https://issues.apache.org/jira/browse/CARBONDATA/?selectedTab=com.atlassian.jira.jira-projects-plugin:versions-panel). 
Triage each unresolved issue with one of the following resolutions:

* If the issue has been resolved and Jira was not updated, resolve it accordingly.
* If the issue has not been resolved and it is acceptable to defer until the next release, update
 the `Fix Version` field to the new version you just created. Please consider discussing this 
 with stakeholders and the dev@ mailing list, as appropriate.
* If the issue has not been resolved and it is not acceptable to release until it is fixed, the 
 release cannot proceed. Instead, work with the CarbonData community to resolve the issue.
 
#### Review Release Notes in Jira

Jira automatically generates Release Notes based on the `Fix Version` applied to the issues. 
Release Notes are intended for CarbonData users (not CarbonData committers/contributors). You 
should ensure that Release Notes are informative and useful.

Open the release notes from the [version status page](https://issues.apache.org/jira/browse/CARBONDATA/?selectedTab=com.atlassian.jira.jira-projects-plugin:versions-panel)
by choosing the release underway and clicking Release Notes.

You should verify that the issues listed automatically by Jira are appropriate to appear in the 
Release Notes. Specifically, issues should:

* Be appropriate classified as `Bug`, `New Feature`, `Improvement`, etc.
* Represent noteworthy user-facing changes, such as new functionality, backward-incompatible 
changes, or performance improvements.
* Have occurred since the previous release; an issue that was introduced and fixed between 
releases should not appear in the Release Notes.
* Have an issue title that makes sense when read on its own.

Adjust any of the above properties to the improve clarity and presentation of the Release Notes.

#### Verify that a Release Build works

Run `mvn clean install -Prelease` to ensure that the build processes that are specific to that 
profile are in good shape.

_Checklist to proceed to the next step:_

1. Release Manager's GPG key is published to `dist.apache.org`.
2. Release Manager's GPG key is configured in `git` configuration.
3. Release Manager has `org.apache.carbondata` listed under `Staging Profiles` in Nexus.
4. Release Manager's Nexus User Token is configured in `settings.xml`.
5. Jira release item for the subsequent release has been created.
6. There are no release blocking Jira issues.
7. Release Notes in Jira have been audited and adjusted.

### Build a release

Use Maven release plugin to tag and build release artifacts, as follows:

```
mvn release:prepare
```

Use Maven release plugin to stage these artifacts on the Apache Nexus repository, as follows:

```
mvn release:perform
```

Review all staged artifacts. They should contain all relevant parts for each module, including 
`pom.xml`, jar, test jar, source, etc. Artifact names should follow 
[the existing format](https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22org.apache.carbondata%22)
in which artifact name mirrors directory structure. Carefully review any new artifacts.

Close the staging repository on Nexus. When prompted for a description, enter "Apache CarbonData 
x.x.x release".

### Stage source release on dist.apache.org

Copy the source release to dev repository on `dist.apache.org`.

1. If you have not already, check out the Incubator section of the `dev` repository on `dist
.apache.org` via Subversion. In a fresh directory:

```
svn co https://dist.apache.org/repos/dist/dev/incubator/carbondata
```

2. Make a directory for the new release:

```
mkdir x.x.x
```

3. Copy the CarbonData source distribution, hash, and GPG signature:

```
cp apache-carbondata-x.x.x-source-release.zip x.x.x
```

4. Add and commit the files:

```
svn add x.x.x
svn commit
```

5. Verify the files are [present](https://dist.apache.org/repos/dist/dev/incubator/carbondata).

### Propose a pull request for website updates

The final step of building a release candidate is to propose a website pull request.

This pull request should update the following page with the new release:

* `src/main/webapp/index.html`
* `src/main/webapp/docs/latest/mainpage.html`

_Checklist to proceed to the next step:_

1. Maven artifacts deployed to the staging repository of 
[repository.apache.org](https://repository.apache.org)
2. Source distribution deployed to the dev repository of
[dist.apache.org](https://dist.apache.org/repos/dist/dev/incubator/carbondata/)
3. Website pull request to list the release.

## Vote on the release candidate

Once you have built and individually reviewed the release candidate, please share it for the 
community-wide review. Please review foundation-wide [voting guidelines](http://www.apache.org/foundation/voting.html)
for more information.

Start the review-and-vote thread on the dev@ mailing list. Here's an email template; please 
adjust as you see fit:

```
From: Release Manager
To: dev@carbondata.incubator.apache.org
Subject: [VOTE] Apache CarbonData Release x.x.x

Hi everyone,
Please review and vote on the release candidate for the version x.x.x, as follows:

[ ] +1, Approve the release
[ ] -1, Do not approve the release (please provide specific comments)

The complete staging area is available for your review, which includes:
* JIRA release notes [1],
* the official Apache source release to be deployed to dist.apache.org [2], which is signed with the key with fingerprint FFFFFFFF [3],
* all artifacts to be deployed to the Maven Central Repository [4],
* source code tag "x.x.x" [5],
* website pull request listing the release [6].

The vote will be open for at least 72 hours. It is adopted by majority approval, with at least 3 PMC affirmative votes.

Thanks,
Release Manager

[1] link
[2] link
[3] https://dist.apache.org/repos/dist/dist/incubator/carbondata/KEYS
[4] link
[5] link
[6] link
```

If there are any issues found in the release candidate, reply on the vote thread to cancel the vote.
There’s no need to wait 72 hours. Proceed to the `Cancel a Release (Fix Issues)` step below and 
address the problem.
However, some issues don’t require cancellation.
For example, if an issue is found in the website pull request, just correct it on the spot and the
vote can continue as-is.

If there are no issues, reply on the vote thread to close the voting. Then, tally the votes in a
separate email. Here’s an email template; please adjust as you see fit.

```
From: Release Manager
To: dev@carbondata.incubator.apache.org
Subject: [RESULT][VOTE] Apache CarbonData Release x.x.x

I'm happy to announce that we have unanimously approved this release.

There are XXX approving votes, XXX of which are binding:
* approver 1
* approver 2
* approver 3
* approver 4

There are no disapproving votes.

Thanks everyone!
```

While in incubation, the Apache Incubator PMC must also vote on each release, using the same 
process as above. Start the review and vote thread on the `general@incubator.apache.org` list.

```
From: Release Manager
To: general@incubator.apache.org
Cc: dev@carbondata.incubator.apache.org
Subject: [VOTE] Apache CarbonData release x.x.x-incubating

Hi everyone,
Please review and vote on the release candidate for the Apache CarbonData version x.x.x-incubating,
 as follows:
 
[ ] +1, Approve the release
[ ] -1, Do not approve the release (please provide specific comments)

The complete staging area is available for your review, which includes:
* JIRA release notes [1],
* the official Apache source release to be deployed to dist.apache.org [2],
* all artifacts to be deployed to the Maven Central Repository [3],
* source code tag "x.x.x" [4],
* website pull request listing the release [5].

The Apache CarbonData community has unanimously approved this release [6].

As customary, the vote will be open for at least 72 hours. It is adopted by
a majority approval with at least three PMC affirmative votes. If approved,
we will proceed with the release.

Thanks!

[1] link
[2] link
[3] link
[4] link
[5] link
[6] lists.apache.org permalink to the vote result thread, e.g.,  https://lists.apache.org/thread
.html/32c991987e0abf2a09cd8afad472cf02e482af02ac35418ee8731940@%3Cdev.carbondata.apache.org%3E
```

If passed, close the voting and summarize the results:
 
```
From: Release Manager
To: general@incubator.apache.org
Cc: dev@carbondata.incubator.apache.org
Subject: [RESULT][VOTE] Apache CarbonData release x.x.x-incubating

There are XXX approving votes, all of which are binding:
* approver 1
* approver 2
* approver 3
* approver 4

There are no disapproving votes.

We'll proceed with this release as staged.

Thanks everyone!
```

_Checklist to proceed to the final step:_

1. Community votes to release the proposed release
2. While in incubation, Apache Incubator PMC votes to release the proposed release

## Cancel a Release (Fix Issues)

Any issue identified during the community review and vote should be fixed in this step.

To fully cacel a vote:

* Cancel the current release and verify the version is back to the correct SNAPSHOT:

```
mvn release:cancel
```

* Drop the release tag:

```
git tag -d x.x.x
git push --delete apache x.x.x
```

* Drop the staging repository on Nexus ([repository.apache.org](https://repository.apache.org))


Verify the version is back to the correct SNAPSHOT.

Code changes should be proposed as standard pull requests and merged.

Once all issues have been resolved, you should go back and build a new release candidate with 
these changes.

## Finalize the release

Once the release candidate has been reviewed and approved by the community, the release should be
 finalized. This involves the final deployment of the release to the release repositories, 
 merging the website changes, and announce the release.
 
### Deploy artifacts to Maven Central repository

On Nexus, release the staged artifacts to Maven Central repository. In the `Staging Repositories`
 section, find the relevant release candidate `orgapachecarbondata-XXX` entry and click `Release`.

### Deploy source release to dist.apache.org

Copy the source release from the `dev` repository to `release` repository at `dist.apache.org` 
using Subversion.

### Merge website pull request

Merge the website pull request to list the release created earlier.

### Mark the version as released in Jira

In Jira, inside [version management](https://issues.apache.org/jira/plugins/servlet/project-config/CARBONDATA/versions)
, hover over the current release and a settings menu will appear. Click `Release`, and select 
today's state.

_Checklist to proceed to the next step:_

1. Maven artifacts released and indexed in the
 [Maven Central repository](https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22org.apache.carbondata%22)
2. Source distribution available in the release repository of
 [dist.apache.org](https://dist.apache.org/repos/dist/release/incubator/carbondata/)
3. Website pull request to list the release merged
4. Release version finalized in Jira

## Promote the release

Once the release has been finalized, the last step of the process is to promote the release 
within the project and beyond.

### Apache mailing lists

Announce on the dev@ mailing list that the release has been finished.
 
Announce on the user@ mailing list that the release is available, listing major improvements and 
contributions.

While in incubation, announce the release on the Incubator's general@ mailing list.

_Checklist to declare the process completed:_

1. Release announced on the user@ mailing list.
2. Release announced on the Incubator's general@ mailing list.
3. Completion declared on the dev@ mailing list.
