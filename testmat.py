import scipy.io as scio
import os
import pickle
rootfolder = 'C:\\Users\\THU-EE-WL\\Desktop\\EEG_feature_examples'
path = os.path.join(rootfolder, 'chan_abspower_dB.mat')
a = scio.loadmat(path)
print(type(a))
a.pop('__header__')
a.pop('__version__')
a.pop('__globals__')
print(a['chan_abspower']['deltapower'])

for k in a.keys():
    print(k)
    """
    print(a[k][0, 0]['deltapower'])
    print(a[k][0, 0][0])
    print(a[k][0].shape)
    """
