#!/usr/bin/env bash

# Legacy, keep here if we need to use Pave DB Connector
#sudo docker run -v /home/langston/pave-prism/pave-agent/pa-server-ca.pem:/pa-server-ca.pem -v /home/langston/pave-prism/pave-agent/pa-client-cert.pem:/pa-client-cert.pem -v /home/langston/pave-prism/pave-agent/pa-client-key.pem:/pa-client-key.pem --env-file /home/langston/pave-prism/pave-agent/.env -p 8123:8000 docker.io/pavedev/agent:latest

sudo docker kill pave-agent
sudo docker rm pave-agent
sudo docker run -d --name pave-agent --env-file /home/langston/pave-prism/pave-agent.env -p 8123:8000 docker.io/pavedev/agent:latest
