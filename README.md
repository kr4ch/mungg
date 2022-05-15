# mungg

Test Repo for BULA Post Sortier App

This is a docker repository that is automatically built and pushed to docker hub, when a version is tagged as `v*.*.*`
https://hub.docker.com/r/krach/mungg

```
 git tag -a v0.0.2
 git push origin v0.0.2
```

Currently contains:
* mysql database
* python flask webserver
* basic code to interact with the db


# Testing the application online
* Go to https://labs.play-with-docker.com/
* Start a session
* Click "Create new instance"
* In the command line enter: `docker run -dp 8000:5000 krach/mungg`
* Click on "Open Port" and enter `8000`
* This should open a new tab with the application

alternatively install "Docker Desktop", get this repo `docker pull krach/mungg` and run it locally
