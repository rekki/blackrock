VERSION=0.16
sudo docker build -t jackdoe/blackrock:$VERSION . --no-cache
sudo docker push jackdoe/blackrock:$VERSION
