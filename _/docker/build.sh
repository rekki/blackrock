VERSION=0.27
sudo docker build -t jackdoe/blackrock:$VERSION . --no-cache
sudo docker push jackdoe/blackrock:$VERSION
