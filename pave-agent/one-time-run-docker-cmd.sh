export GOOGLE_APPLICATION_CREDENTIALS=/home/langston/pave-prism/pave-agent/creds.json
sudo docker run --name pave-agent-one-time --rm  -v /home/langston/pave-prism/pave-agent/old_certs/pa-server-ca.pem:/pa-server-ca.pem -v /home/langston/pave-prism/pave-agent/old_certs/pa-client-cert.pem:/pa-client-cert.pem -v /home/langston/pave-prism/pave-agent/old_certs/pa-client-key.pem:/pa-client-key.pem --env-file /home/langston/pave-prism/pave-agent/one-time-run.env -p 8124:8000 docker.io/pavedev/agent

