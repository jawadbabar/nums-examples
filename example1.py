import nums.core.settings as settings
from nums.core.application_manager import instance
from nums.core.array.blockarray import BlockArray, Block
from nums.core.array.application import ArrayApplication


app: ArrayApplication = instance()

X = app.random.normal(loc=1.0, scale=10.0, shape=(100,), block_shape=(23,))
Y = app.random.normal(loc=1.0, scale=10.0, shape=(100,), block_shape=(23,))


Z = X + Y
print("")
print(Z.shape, Z.grid_shape, Z.block_shape)
print(Z)
print(Z.get())