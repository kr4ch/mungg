# mungg

Test Repo for BULA Post Sortier App

This is a docker repository that is automatically built and pushed to docker hub, when a version is tagged as v*.*.*
https://hub.docker.com/r/krach/mungg

```
 git tag -a v0.0.2
 git push origin v0.0.2
```

Currently contains:
* mysql database
* python flask webserver
* basic code to interact with the db