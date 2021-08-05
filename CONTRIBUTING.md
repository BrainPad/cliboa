# How to Contribute to cliboa

## Contribution Policy

* If you found a bug, would like you to fix the bug by yourself.
* If you want a new feature, would like you to implement it by yourself.
* If you would like to discuss or have doubts before implement it, would like you to make issues.


## Workflow for External Developers
### Bugfix or hotfix or New feature
1. Fork from https://github.com/BrainPad/cliboa
2. Create your branch for bugfix or hotfix or feature 
```
$ git checkout -b new-feature
```
3. Commit your changes 
```
$ git commit -am 'Add some feature'
```
4. Push to your branch 
```
$ git push origin new-feature
```
5. Create new pull request to https://github.com/BrainPad/cliboa, master branch


## Workflow for BrainPad Developers
### Development at usual work
Use <a href="https://guides.github.com/introduction/flow/">github flow</a>

![](/img/cliboa_github_flow.png)

1. Create development branch from master
2. Commit your changes
3. Push to the branch
4. Create new pull request to master branch


## Workflow for All Developers According to Roadmap
Prepare the branch by each versions before development

![](/img/cliboa_roadmap_flow.png)

1. Create development branch from version branch
2. Commit your changes
3. Push to the branch
4. Create new pull request to version branch
