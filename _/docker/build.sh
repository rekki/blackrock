VERSION=0.14
sudo docker build -t jackdoe/blackrock:$VERSION . --no-cache
sudo docker push jackdoe/blackrock:$VERSION
