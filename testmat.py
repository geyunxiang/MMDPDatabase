import scipy.io as scio
import os
import pickle
rootfolder = 'C:\\Users\\THU-EE-WL\\Desktop\\EEG_feature_examples'
print(os.listdir(rootfolder))
path = os.path.join(rootfolder, 'chan_abspower_dB.mat')
a = scio.loadmat(path)
print(type(a))
print(a)
a.pop('__header__')
a.pop('__version__')
a.pop('__globals__')
for k in a.keys():
    a[k] = pickle.dumps(a[k])
print(a)
