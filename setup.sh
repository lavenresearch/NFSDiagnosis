#!/bin/sh

cd ./libs/
echo $(pwd)

# install tshark

rpm -i libsmi-0.4.8-4.fc14.x86_64.rpm
rpm -i GeoIP-1.4.7-0.1.20090931cvs.fc12.x86_64.rpm
rpm -i wireshark-1.4.0-2.fc14.x86_64.rpm

# install zookeeper kafka and python kafka

chmod +x *.sh
./install_kafka.sh
./install_python_kafka.sh

echo "setup finished!"
