FROM alpine:3.18


#base packages
RUN apk update
RUN apk add py3-pip

#zfs autobackup tests dependencies
RUN apk add zfs openssh lzop pigz zstd gzip xz lz4 mbuffer udev zfs-udev


#python modules
COPY requirements.txt /
RUN pip3 install -r requirements.txt

#git repo should be mounted in /app:
ENTRYPOINT [ "/app/tests/tests_docker" ]
