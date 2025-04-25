import numpy as np

data = [1, 2, 3, 4, 5]
arr = np.array(data)
print(arr)

##列表和元组
a = list()
b = []
c = list(range(1, 6))
d=[1,3,4,6]
print("a:", a)
print("b:", b)
print("c:", c)
print("d:", d)
print(type(a))
# 输出: a: []
# 输出: b: []
# 输出: c: [1, 2, 3, 4, 5]

##使用 arange() 创建等差数组
arr = np.arange(0, 10, 2)
print(arr)


##使用 zeros() 创建全零数组
arr_zeros = np.zeros((3, 4))
print(arr_zeros)



arr_ones = np.ones((2, 5), dtype=int)
print(arr_ones)


arr_linspace = np.linspace(0, 1, 5)
print(arr_linspace)

arr = np.array([[1, 2, 3], [4, 5, 6]])
shape = arr.shape
print(arr)
print("数组形状:", shape)


arr = np.array([1.0, 2.0, 3.0])
dtype = arr.dtype
print(arr)
print("数组数据类型:", dtype)


arr_3d = np.array([[[1, 2], [3, 4]], [[5, 6], [7, 8]]])
ndim = arr_3d.ndim
size = arr_3d.size
print(arr_3d)
print("数组维度:", ndim)
print("数组元素个数:", size)


arr = np.arange(10)
element = arr[5]
print(arr)
print("索引为 5 的元素:", element)


arr_2d = np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
element = arr_2d[1, 2]
print(arr_2d)
print("第二行第三列的元素:", element)


arr = np.arange(10)
slice_arr = arr[2:7]
print(arr)
print("切片结果:", slice_arr)


arr_2d = np.array([[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]])
slice_arr = arr_2d[:2, 1:3]
print(arr_2d)
print("切片结果:")
print(slice_arr)









