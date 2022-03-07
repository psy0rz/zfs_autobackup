import os.path
import os
import time
from random import random

with open('test.py', 'rb') as fh:

    # fsize = fh.seek(10000, os.SEEK_END)
    # print(fsize)

    start=time.time()
    for i in range(0,1000000):
        # fh.seek(0, 0)
        fsize=fh.seek(0, os.SEEK_END)
        # fsize=fh.tell()
        # os.path.getsize('test.py')
    print(time.time()-start)


    print(fh.tell())

sys.exit(0)



checked=1
skipped=1
coverage=0.1

max_skip=0


skipinarow=0
while True:
    total=checked+skipped

    skip=coverage<random()
    if skip:
        skipped = skipped + 1
        print("S {:.2f}%".format(checked * 100 / total))

        skipinarow = skipinarow+1
        if skipinarow>max_skip:
            max_skip=skipinarow
    else:
        skipinarow=0
        checked=checked+1
        print("C {:.2f}%".format(checked * 100 / total))

    print(max_skip)

skip=0
while True:

    total=checked+skipped
    if skip>0:
        skip=skip-1
        skipped = skipped + 1
        print("S {:.2f}%".format(checked * 100 / total))
    else:
        checked=checked+1
        print("C {:.2f}%".format(checked * 100 / total))

        #calc new skip
        skip=skip+((1/coverage)-1)*(random()*2)
        # print(skip)
        if skip> max_skip:
            max_skip=skip

    print(max_skip)
