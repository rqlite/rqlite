# Contributing to rqlite
For full details on developing, and contributing to, rqlite, check out [rqlite.io](https://rqlite.io/docs/install-rqlite/building-from-source/). rqlite can be compiled and executed on Linux, macOS, and Microsoft Windows.

rqlite is software, and it goes without saying it can always be improved. It's by no means finished -- issues are tracked, and I plan to develop this project further. Pull requests are welcome, though larger proposals should be discussed first. The design and implementation of rqlite is somewhat opinionated conservative however, so feature and design changes may be slow to become part of rqlite. 

## Design information
Many packages have their own `DESIGN.md` design document. You should review these before making changes. Doing so will help you understand the code and its construction.

## Issues are not assigned
Issues are never explicitly assigned to individuals. If you wish to work on issue simply ask questions on the issue as needed, and generate a Pull Request with your proposed fix. **Before coding any substantial change it's strongly recommended you discuss your proposal first**.

## Use of Coding Agents
Coding Agents are fine to use, but any PR that appears to be "AI slop" or generated without any apparent thought by the actual programmer, may be closed without comment. If you open an issue or PR that includes analysis by a Coding Agent please state up front that it is agent-generated.

### Judgement still required
Simply pointing an Agent at a feature request or bug report, and having it generate a PR, is unlikely to be an effective working style. Why? For two reasons:
- The PR is liable to be quite large. Reviewing a large PR for bugs, code style issues, and coherence with the rest of the codebase, is difficult. A large PR may never be reviewed properly, and therefore never merged. This problem predates Agent-enabled coding.
- Lack of judgement. If you didn't actually write the code, then you may not understand **why** certain coding choices were made. If your response to comment on the PR is to simply feed them back into your Agent and post **its** response, then you've become _a passthrough for a model_. In that case the maintainers might as well drive the Agent themselves. This problem is a new one.

Breaking changes into smaller pieces will help get your contribution merged quicker. There has never been a large feature in rqlite that was not composed of a small series of changes. So for a large change you should propose what series of changes you will make. But the same requirements apply -- you must show that you can argue from first principles **why** a certain series of changes should take place.
