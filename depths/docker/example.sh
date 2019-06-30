ROOT=/tmp/jubei
TOPIC=blackrock
IMAGE=jackdoe/blackrock:0.1
KAFKA=localhost:9092
VERBOSE=-verbose

#orgrim
sudo docker run -d -p 7000:7000 $IMAGE orgrim -bind :7000 -topic-data $TOPIC -kafka $KAFKA $VERBOSE

#khanzo
sudo docker run -d -p 7001:7001 -v $ROOT:/blackrock $IMAGE khanzo -bind :7001 -root /blackrock -topic-data $TOPIC $VERBOSE

#blackhand
sudo docker run -d -p 7002:7002 $IMAGE blackhand -bind :7002 -topic-data $TOPIC -kafka $KAFKA $VERBOSE

#jubei
sudo docker run -d -v $ROOT:/blackrock $IMAGE jubei -root /blackrock -topic-data $TOPIC -kafka $KAFKA $VERBOSE


