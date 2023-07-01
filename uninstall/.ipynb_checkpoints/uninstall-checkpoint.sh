#!/bin/bash

sudo rm -r ../data

sudo rm ../creds.json

sudo apt-get --purge -y remove postgresql postgresql-*

list=$(dpkg -l | grep postgres | awk '{print $2}')

for i in $list
do
    sudo apt-get -y --purge remove $i
done
