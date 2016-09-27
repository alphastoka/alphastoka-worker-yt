#!/bin/bash
#
echo $1
docker build -t stoka-yt .
docker tag stoka-yt ssabpisa/stoka-yt:$1
docker push ssabpisa/stoka-yt:$1