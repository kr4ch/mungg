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
Click this button to test it with "play-with-docker.com":
<a href="https://labs.play-with-docker.com/?stack=https://raw.githubusercontent.com/kr4ch/mungg/main/docker-compose.test.yml"><img src="https://cdn.rawgit.com/play-with-docker/stacks/cff22438/assets/images/button.png" alt="Try in PWD"></a>
* Click "Start"
* Wait a few minutes
* Click "Close" (if close button does not work, click on the blue banner and hit "Esc")
* Click on the button "8000" Next to "Open Port"
* This should open a new tab with the application

# Testing the application locally
Install "Docker Desktop" and run it locally:
```
docker pull krach/mungg
git clone git@github.com:kr4ch/mungg.git
cd mungg
docker-compose docker-compose.test.yml up
```
* Open "http://localhost:8000" in a browser
