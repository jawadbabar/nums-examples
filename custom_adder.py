from nums.core.application_manager import instance
from nums.core.array.blockarray import BlockArray, Block
from nums.core.array.application import ArrayApplication

import ray

ray.init(num_cpus=2)


# Approach 1: bringing both operands to driver and conducting add operation on driver
# (slow, not parallel)
def custom_sum_1(left: BlockArray, right: BlockArray):
    custom_res = []
    for x, y in zip(left.blocks, right.blocks):
        for a, b in zip(x.get(), y.get()):
            custom_res.append(a + b)
    return custom_res


# Approach 2: doing computation remotely with a ray task
# (faster, parallel)
@ray.remote
def block_add(left, right):
    return [l + r for l, r in zip(left, right)]


def custom_sum_2(left: BlockArray, right: BlockArray):
    obj_refs = []
    for x, y in zip(left.blocks, right.blocks):
        obj_refs.append(block_add.remote(x.oid, y.oid))
    return obj_refs


# useful function to flatten a list of list
def flatten(t):
    return [item for sublist in t for item in sublist]


app: ArrayApplication = instance()

X: BlockArray = app.random.normal(loc=1.0, scale=10.0, shape=(100,), block_shape=(23,))
Y: BlockArray = app.random.normal(loc=1.0, scale=10.0, shape=(100,), block_shape=(23,))

Z: BlockArray = X + Y

# built-in 'add'
blockarray_result = Z.get()

# custom not parallel 'add'
custom_result_1 = custom_sum_1(X, Y)

# custom parallel 'add'
custom_result_2 = flatten(ray.get(custom_sum_2(X, Y)))

# verifying results
for realised1, realised2, expected in zip(custom_result_1, custom_result_2, blockarray_result):
    if realised1 != expected or realised2 != expected:
        print("realised 1: " + str(realised1))
        print("realised 2: " + str(realised2))
        print("expected: " + str(expected))

