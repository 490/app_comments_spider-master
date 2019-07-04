from settings import BLOOMFILTER_BIT, BLOOMFILTER_HASH_NUMBER


class HashMap(object):
    def __init__(self, m, seed):
        self.m = m
        self.seed = seed

    def hash(self, value):
        """
        哈希算法
        :param value:
        :return:
        """
        ret = 0
        for i in range(len(value)):
            ret += self.seed * ret + ord(value[i])
        return (self.m - 1) & ret


class BloomFilter(object):
    def __init__(self, server, key, bit=BLOOMFILTER_BIT, hash_number=BLOOMFILTER_HASH_NUMBER):
        """
        Initialize BloomFilter
        :param server: Redis Server
        :param key: BloomFilter Key
        :param bit: m = 2 ^ bit
        :param hash_number: the number of hash function
        """
        self.m = 1 << bit
        self.seeds = range(hash_number)
        self.server = server
        self.key = key
        self.maps = [HashMap(self.m, seed) for seed in self.seeds]

    def exists(self, value):
        """
        if value exists
        :param value:
        :return:
        判定是否重复。方法参数value为待判断的元素。先定义变量exist。
        遍历所有散列函数对value进行散列运算，得到映射位置。
        用getbit()方法取得该映射位置的结果，循环进行与运算。
        只有当每次getbit()得到的结果都为1时，最后的exist才为True，即代表value属于这个集合。
        如只要有一次getbit()得到的结果为0，即m位数组中有对应的0位，那么最终的结果exist就为False，即代表value不属于这个集合。
        """
        if not value:
            return False
        exist = True
        for map in self.maps:
            offset = map.hash(value)  # 与运算，只要有一个0，exist就为0
            exist = exist & self.server.getbit(self.key, offset)
        return exist

    def insert(self, value):
        """
        add value to bloom
        :param value:
        :return:
        遍历初始化好的散列函数，调用其hash()方法算出映射位置offset，
        再利用Redis的setbit()方法将该位置1。
        """
        for f in self.maps:
            offset = f.hash(value)
            self.server.setbit(self.key, offset, 1)
