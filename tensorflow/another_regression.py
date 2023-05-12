import tensorflow as tf
import numpy as np


np.random.seed(0)
X = np.random.randn(100, 3)
y = np.random.randn(100, 1)

model = tf.keras.Sequential([
    tf.keras.layers.Dense(1, input_shape=(3,))
])

model.compile(optimizer='sgd', loss='mse')

history = model.fit(X, y, epochs=100, verbose=0)

loss = model.evaluate(X, y, verbose=0)
print('Loss:', loss)
