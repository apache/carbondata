#!/bin/sh
eval "$(ssh-agent -s)"
ssh-add /opt/rsa_key

cd ../
#git reset --hard origin/master
#git clean -f
#git pull

git stash
git clean -f -d
git clean -f 
git checkout Carbon_V1R2C20_Development
git pull origin Carbon_V1R2C20_Development



find . -iname *.sh | xargs chmod +x
pid=`pidof ssh-agent -s`
kill -9 $pid

