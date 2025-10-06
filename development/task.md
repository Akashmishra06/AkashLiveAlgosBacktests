# Let's generate a text file containing all the mentioned NumPy functions
# Each with a one-line explanation.

functions_text = """
# 1. Conditional & Logical Operations
np.where(cond, x, y): Element-wise if/else operation.
np.select([conds], [choices], default): Apply multiple if-else conditions.
np.logical_and(a, b): Element-wise logical AND operation.
np.logical_or(a, b): Element-wise logical OR operation.
np.logical_not(a): Element-wise logical NOT operation.
np.isin(a, values): Checks if elements of array exist in a list.
np.any(): Returns True if any element is True.
np.all(): Returns True if all elements are True.

# 2. Array Creation & Initialization
np.array(list): Create a NumPy array from a list or sequence.
np.arange(start, stop, step): Generate evenly spaced values.
np.linspace(start, stop, num): Generate evenly spaced numbers over a range.
np.zeros(shape): Create array of zeros.
np.ones(shape): Create array of ones.
np.full(shape, fill_value): Create array filled with specific value.
np.eye(n): Create an identity matrix.

# 3. Statistical & Math Functions
np.mean(): Compute the mean of array elements.
np.median(): Compute the median of array elements.
np.std(): Compute the standard deviation.
np.var(): Compute the variance.
np.min(): Find the minimum value.
np.max(): Find the maximum value.
np.percentile(arr, q): Compute the q-th percentile.
np.diff(arr): Compute difference between consecutive elements.
np.cumsum(): Compute cumulative sum.
np.cumprod(): Compute cumulative product.
np.nanmean(): Mean ignoring NaN values.
np.nanstd(): Standard deviation ignoring NaN values.

# 4. Handling NaN / Missing Data
np.isnan(arr): Detect NaN values in array.
np.nan_to_num(arr, nan=value): Replace NaN with a given value.
np.where(np.isnan(arr), fill, arr): Replace NaN conditionally.

# 5. Array Manipulation
np.reshape(arr, shape): Change array shape.
np.concatenate([a, b]): Join arrays along an axis.
np.vstack(): Stack arrays vertically.
np.hstack(): Stack arrays horizontally.
np.unique(arr, return_counts=True): Get unique elements and their counts.
np.argsort(arr): Return indices that would sort array.
np.take(arr, indices): Take elements by specific indices.

# 6. Vectorized Math
np.abs(): Compute absolute values.
np.round(): Round elements to nearest integer.
np.exp(): Compute exponential of all elements.
np.log(): Compute natural logarithm.
np.sqrt(): Compute square root.
np.power(a, b): Raise elements to power b.
np.clip(a, min, max): Limit array values within a range.
np.sign(): Return sign of numbers (-1, 0, 1).

# 7. Random & Sampling
np.random.rand(n): Generate n random numbers between 0 and 1.
np.random.randn(n): Generate n samples from standard normal distribution.
np.random.choice(a, size, replace): Randomly sample elements.
np.random.seed(seed): Fix random generator for reproducibility.

# 8. Performance Helpers
np.vectorize(func): Vectorize a Python function for element-wise operation.
np.apply_along_axis(func, axis, arr): Apply function along a specific axis.
"""

file_path = "/mnt/data/numpy_functions_summary.txt"

with open(file_path, "w") as f:
    f.write(functions_text)

file_path
