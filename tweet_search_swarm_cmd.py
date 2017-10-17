sudo docker pull lixiepeng/tweet_search
sudo docker run -ti --name worker lixiepeng/tweet_search python worker_test.py
sudo docker rm worker
sudo docker service rm worker
sudo docker service create --name worker --replicas=8 lixiepeng/tweet_search python worker_test.py
sudo docker service ls
sudo docker service rm worker