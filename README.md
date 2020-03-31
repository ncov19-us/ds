# <img src="covid19-logo.png" width="25px" height="25px">[COVID19 US Specific Dashboard](https://ncov19.us/)

# DS - Analytics, and Data Munging Repo

# Data Sources

- For US and State [Johns Hopkins CSSE](https://github.com/CSSEGISandData/COVID-19)
- For County comes from State & Local Govs. County Sites
- For Tested Data: [COVIDTRACKING](https://covidtracking.com/api/)
- Drive-Thru COVID-19 Testing Centers, hand labelled from News Articles
- Twitter API
- News API
- Google News API

## Git Workflow

- Our commits use a style called **[semantic commits](https://seesparkbox.com/foundry/semantic_commit_messages)**. 
- Pick a task to work on from the Trello board, assign your name to it via the top right of the Trello card, and confirm that no one is already working on this task, or another task that is likely to cause merge issues with yours.

```sh
git clone https://github.com/ncov19-us/ncov19-vacc-dash-front-end.git
git checkout <staging-branch>
git checkout -b <your-feature-branch>
```
Commit your work using semantic commits structure.

When done:
```sh
git checkout <staging-branch>
git pull  # sync your local feature branch with origin/github
git checkout <your-feature-branch>
git rebase <staging-branch>  # merge your branch w/ feature-branch
git commit
```

Push your branch: `git push origin <your-feature-branch>`. Then open a pull request from your working branch into `staging-branch` for review.

**If you do not have experience working with GitHub, or are confused about certain instructions, please message your project lead.**
- Create a **new feature branch** with a name that accurately describes the task you are working on, and commit in a way that follows semantic guidelines. _DO NOT COMMIT OR MERGE TO MASTER_
- When you complete a feature and it is **bug-free**, create a pull request to merge into **staging**, _NOT MASTER_, and request a review from at minimum your project lead.  It is a good idea to have other team members review your code as well.
- _Your project lead_ will be the one to approve your PR. Should they make any comments/merge conflicts arise, please be responsive and communicate with them.
- Once the PR has been successfully merged, you can delete the feature branch on GitHub (if all work on that branch is complete) and move your task's Trello card to _the appropriate "completed" column_ on the Trello board.
- Remember, do not hesitate to ask questions. Questions now are better than merge conflicts later!

## Original Contributors

| [Harsh Desai](https://github.com/hurshd0)     | [Elizabeth Ter Sahakyan](https://github.com/elizabethts) | [Han Lee](https://github.com/leehanchung) | [Alex Pakalniskis](https://github.com/alex-pakalniskis) |
| :--------------------: | :--------------------: | :--------------------: | :--------------------: | 
| <img src="https://avatars2.githubusercontent.com/u/16807421?s=400&u=844b3a27a223f7e3e2b3318e6a917d3641f93d6a&v=4" width = "200" /> | <img src="https://avatars1.githubusercontent.com/u/30808123?s=400&u=7757b1986b1e1713f378b402cb4e0a43b33ed451&v=4" width = "200" /> | <img src="https://avatars2.githubusercontent.com/u/4794839?s=400&u=1b4ce1a3a102b472ceaeae0f7f5b45df39f80322&v=4" width = "200" /> | <img src="https://avatars1.githubusercontent.com/u/43630382?s=460&u=258906c39825009e18e3962cd08c5d3776521e9b&v=4" width="200"/> |
| Data Scientist | Data Scientist | Machine Learning Engineer | Data Scientist |
| [<img src="https://github.com/favicon.ico" width="20"> ](https://github.com/hurshd0) [ <img src="https://static.licdn.com/sc/h/al2o9zrvru7aqj8e1x2rzsrca" width="20"> ](https://www.linkedin.com/in/hurshd/)                   |[<img src="https://github.com/favicon.ico" width="20"> ](https://github.com/elizabethts) [ <img src="https://static.licdn.com/sc/h/al2o9zrvru7aqj8e1x2rzsrca" width="20"> ](https://www.linkedin.com/in/elizabethts/)    |[<img src="https://github.com/favicon.ico" width="20"> ](https://github.com/leehanchung) [ <img src="https://static.licdn.com/sc/h/al2o9zrvru7aqj8e1x2rzsrca" width="20"> ](https://www.linkedin.com/in/hanchunglee/)   | [<img src="https://github.com/favicon.ico" width="20"> ](https://github.com/alex-pakalniskis) [ <img src="https://static.licdn.com/sc/h/al2o9zrvru7aqj8e1x2rzsrca" width="20"> ](https://www.linkedin.com/in/alexpakalniskis3/)   | 

