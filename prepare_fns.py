# copy apps/imgs/fn2.wasm to fn1-fn10
import os

for i in range(10):
    if i==1:
        continue
    os.system("cp apps/imgs/fn2.wasm apps/imgs/fn{}.wasm".format(i+1))