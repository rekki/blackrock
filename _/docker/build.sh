VERSION=0.86
sudo docker build -t jackdoe/blackrock:$VERSION . --no-cache
sudo docker push jackdoe/blackrock:$VERSION
