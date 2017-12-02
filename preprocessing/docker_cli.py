sudo docker ps -aq
sudo docker restart $(sudo docker ps -aq)
sudo docker stop $(sudo docker ps -aq)
sudo docker rm -f $(sudo docker ps -aq)

sudo apt install htop

sudo reboot

sudo systemctl restart docker

sudo docker run -e CATTLE_HOST_LABELS='spider=worker'  --rm --privileged -v /var/run/docker.sock:/var/run/docker.sock -v /var/lib/rancher:/var/lib/rancher rancher/agent:v1.2.6 http://101.132.182.124:8080/v1/scripts/A0411FEE5B3F75EEE4CB:1483142400000:v43bmonJvQcrpYeEmJVn8f5n5I0

