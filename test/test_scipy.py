from scipy.cluster.vq import kmeans, vq, whiten
import numpy as np

# 生成样本数据
data = np.vstack((np.random.rand(100, 3) + np.array([.5, .5, .5]), np.random.rand(100, 3)))
print(data)
data = whiten(data)
print(data.shape )

# 计算聚类中心
centroids, _ = kmeans(data, 2)
print(centroids)

# 分配样本到聚类中心
clx, dist = vq(data, centroids)
print(clx)