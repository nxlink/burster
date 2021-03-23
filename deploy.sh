#!/bin/bash
echo "Deploying Burster code"
rsync -av  --exclude-from '/etc/deploy-exclude.txt' /opt/burster/ root@blackbox.nxlink.com:/opt/burster/
