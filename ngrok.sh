#!/bin/bash
if ! [ -e ngrok ]; then
        wget -O ngrok.zip https://bin.equinox.io/c/4VmDzA7iaHb/ngrok-stable-linux-amd64.zip
        unzip ngrok.zip
fi
{
    mkfifo pipe
    echo "Executing nc"
    nc -k -l -v 8888 <pipe | ( while true; do bash >pipe 2>&1; echo "restarting" ;sleep 1; done )
    killall -SIGINT ngrok && echo "ngrok terminated"
} &
{
    echo "Executing ngrok"
    ./ngrok authtoken $NGROK_TOKEN
    ./ngrok tcp 8888 --log=stdout
} &
wait
